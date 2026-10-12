package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
)

const (
	slackUserTTL = 10 * time.Minute
	// usergroups.users.list is Slack Tier 2 (about 20 requests per minute).
	// Sync runs once a minute, so a full per-group read blows the budget.
	maxMemberReadsPerCycle = 15
	defaultRetryAfter      = 30 * time.Second
	maxRetryAfter          = 60 * time.Second
)

// slackAPI is the subset of slack.Client this sync uses.
type slackAPI interface {
	GetUserGroupsContext(ctx context.Context, options ...slack.GetUserGroupsOption) ([]slack.UserGroup, error)
	GetUsersContext(ctx context.Context, options ...slack.GetUsersOption) ([]slack.User, error)
	CreateUserGroupContext(ctx context.Context, userGroup slack.UserGroup, options ...slack.CreateUserGroupOption) (slack.UserGroup, error)
	EnableUserGroupContext(ctx context.Context, userGroup string, options ...slack.EnableUserGroupOption) (slack.UserGroup, error)
	GetUserGroupMembersContext(ctx context.Context, userGroup string, options ...slack.GetUserGroupMembersOption) ([]string, error)
	UpdateUserGroupMembersContext(ctx context.Context, userGroup string, members string, options ...slack.UpdateUserGroupMembersOption) (slack.UserGroup, error)
}

type slackClient struct {
	api   slackAPI
	now   func() time.Time
	sleep func(time.Duration) error

	userTTL time.Duration
	users   []slack.User
	usersAt time.Time

	userGroups []slack.UserGroup

	limitedUntil time.Time
	blocked      bool
	blockErr     error
}

func newSlackClient(api slackAPI, now func() time.Time, sleep func(time.Duration) error) *slackClient {
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = func(d time.Duration) error {
			time.Sleep(d)
			return nil
		}
	}
	return &slackClient{
		api:     api,
		now:     now,
		sleep:   sleep,
		userTTL: slackUserTTL,
	}
}

func (c *slackClient) beginCycle() {
	c.blocked = false
	c.blockErr = nil
}

// do runs a Slack call. On HTTP 429 it waits for Retry-After and tries once
// more. A second 429 blocks further Slack calls until the next sync cycle.
func (c *slackClient) do(call func() error) error {
	if c.blocked {
		return c.blockErr
	}
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if err = c.wait(); err != nil {
			return err
		}
		err = call()
		delay, limited := rateLimitDelay(err)
		if !limited {
			return err
		}
		c.limitedUntil = c.now().Add(delay)
		if attempt == 1 {
			c.blocked = true
			c.blockErr = err
		}
	}
	return err
}

func doValue[T any](c *slackClient, fn func() (T, error)) (T, error) {
	var out T
	err := c.do(func() error {
		var callErr error
		out, callErr = fn()
		return callErr
	})
	return out, err
}

func (c *slackClient) wait() error {
	delay := c.limitedUntil.Sub(c.now())
	if delay <= 0 {
		return nil
	}
	return c.sleep(delay)
}

func rateLimitDelay(err error) (time.Duration, bool) {
	var limited *slack.RateLimitedError
	if !errors.As(err, &limited) {
		return 0, false
	}
	delay := limited.RetryAfter
	if delay <= 0 {
		delay = defaultRetryAfter
	}
	if delay > maxRetryAfter {
		delay = maxRetryAfter
	}
	return delay, true
}

func (c *slackClient) ensureUsers() error {
	if !c.usersAt.IsZero() && c.now().Sub(c.usersAt) < c.userTTL {
		return nil
	}
	users, err := doValue(c, func() ([]slack.User, error) {
		return c.api.GetUsersContext(context.Background())
	})
	if err != nil {
		if useCached(err, len(c.users) > 0, false, "users") {
			return nil
		}
		return err
	}
	c.users = users
	c.usersAt = c.now()
	return nil
}

func (c *slackClient) ensureGroups() error {
	groups, err := c.listGroups(true)
	if err != nil && !isRateLimited(err) {
		logrus.Warnf("slack usergroups.list with members failed, listing groups without members: %v", formatSlackErr(err))
		groups, err = c.listGroups(false)
	}
	if err != nil {
		if useCached(err, len(c.userGroups) > 0, true, "usergroups") {
			return nil
		}
		return err
	}

	previous := make(map[string][]string, len(c.userGroups))
	for _, group := range c.userGroups {
		if group.Users != nil {
			previous[group.ID] = group.Users
		}
	}
	for i := range groups {
		if groups[i].Users == nil {
			if users, ok := previous[groups[i].ID]; ok {
				groups[i].Users = users
			}
		}
	}
	c.userGroups = groups
	return nil
}

func (c *slackClient) listGroups(includeUsers bool) ([]slack.UserGroup, error) {
	return doValue(c, func() ([]slack.UserGroup, error) {
		options := []slack.GetUserGroupsOption{
			slack.GetUserGroupsOptionIncludeDisabled(true),
		}
		if includeUsers {
			options = append(options, slack.GetUserGroupsOptionIncludeUsers(true))
		}
		return c.api.GetUserGroupsContext(context.Background(), options...)
	})
}

func (c *slackClient) idsForEmails(emails []string) (ids []string, missing []string) {
	seen := make(map[string]struct{}, len(emails))
	for _, email := range emails {
		id, ok := c.userID(email)
		if !ok {
			missing = append(missing, email)
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, missing
}

func (c *slackClient) userID(email string) (string, bool) {
	for _, user := range c.users {
		if user.Deleted || user.Profile.Email == "" {
			continue
		}
		if strings.EqualFold(user.Profile.Email, email) {
			return user.ID, true
		}
	}
	return "", false
}

func (c *slackClient) createOrGet(name string) (slack.UserGroup, error) {
	if group, ok := findGroup(c.userGroups, name); ok {
		return group, nil
	}

	created, err := doValue(c, func() (slack.UserGroup, error) {
		return c.api.CreateUserGroupContext(context.Background(), slack.UserGroup{
			Name:   name,
			Handle: name,
		})
	})
	if err == nil {
		if created.Users == nil {
			created.Users = []string{}
		}
		c.putGroup(created)
		return created, nil
	}
	if !isAlreadyExists(err) {
		return slack.UserGroup{}, err
	}
	if refreshErr := c.ensureGroups(); refreshErr != nil && !isRateLimited(refreshErr) {
		return slack.UserGroup{}, refreshErr
	}
	if group, ok := findGroup(c.userGroups, name); ok {
		return group, nil
	}
	return slack.UserGroup{}, err
}

func (c *slackClient) enable(group slack.UserGroup) (slack.UserGroup, error) {
	enabled, err := doValue(c, func() (slack.UserGroup, error) {
		return c.api.EnableUserGroupContext(context.Background(), group.ID)
	})
	if err != nil {
		return slack.UserGroup{}, err
	}
	if enabled.ID == "" {
		enabled.ID = group.ID
	}
	if enabled.Name == "" {
		enabled.Name = group.Name
	}
	if enabled.Handle == "" {
		enabled.Handle = group.Handle
	}
	if enabled.Users == nil {
		enabled.Users = group.Users
	}
	enabled.DateDelete = 0
	c.putGroup(enabled)
	return enabled, nil
}

func (c *slackClient) listMembers(id string) ([]string, error) {
	members, err := doValue(c, func() ([]string, error) {
		return c.api.GetUserGroupMembersContext(context.Background(), id)
	})
	if err != nil {
		return nil, err
	}
	if members == nil {
		members = []string{}
	}
	c.setMembers(id, members)
	return members, nil
}

func (c *slackClient) updateMembers(id string, ids []string) error {
	body := joinIDs(ids)
	return c.do(func() error {
		_, err := c.api.UpdateUserGroupMembersContext(context.Background(), id, body)
		return err
	})
}

func (c *slackClient) setMembers(id string, ids []string) {
	if i := c.groupIndex(id); i >= 0 {
		c.userGroups[i].Users = copyIDs(ids)
	}
}

func (c *slackClient) putGroup(group slack.UserGroup) {
	if i := c.groupIndex(group.ID); i >= 0 {
		if group.Users == nil {
			group.Users = c.userGroups[i].Users
		}
		c.userGroups[i] = group
		return
	}
	c.userGroups = append(c.userGroups, group)
}

func (c *slackClient) groupIndex(id string) int {
	for i := range c.userGroups {
		if c.userGroups[i].ID == id {
			return i
		}
	}
	return -1
}

// findGroup prefers an active handle match. Display names often differ from
// the handle this service created, and a disabled group is absent from
// usergroups.list unless include_disabled is set.
func findGroup(groups []slack.UserGroup, name string) (slack.UserGroup, bool) {
	bestRank := 0
	var best slack.UserGroup
	for i := range groups {
		rank := groupMatchRank(groups[i], name)
		if rank > bestRank {
			bestRank = rank
			best = groups[i]
		}
	}
	return best, bestRank > 0
}

func groupMatchRank(group slack.UserGroup, name string) int {
	handle := group.Handle != "" && strings.EqualFold(group.Handle, name)
	byName := strings.EqualFold(group.Name, name)
	if !handle && !byName {
		return 0
	}
	disabled := group.DateDelete != 0
	switch {
	case handle && !disabled:
		return 4
	case handle:
		return 3
	case !disabled:
		return 2
	default:
		return 1
	}
}

func useCached(err error, hasCache, rateLimitOnly bool, kind string) bool {
	if err == nil || !hasCache || (rateLimitOnly && !isRateLimited(err)) {
		return false
	}
	logrus.Warnf("using cached slack %s after refresh failed: %v", kind, formatSlackErr(err))
	return true
}

func isRateLimited(err error) bool {
	_, ok := rateLimitDelay(err)
	return ok
}

func isAlreadyExists(err error) bool {
	switch slackErrorCode(err) {
	case "handle_already_exists", "name_already_exists", "already_exists":
		return true
	default:
		return false
	}
}

func isInvalidArguments(err error) bool {
	return slackErrorCode(err) == "invalid_arguments"
}

func slackErrorCode(err error) string {
	response, ok := slackError(err)
	if !ok {
		return ""
	}
	return response.Err
}

func slackError(err error) (slack.SlackErrorResponse, bool) {
	var response slack.SlackErrorResponse
	ok := errors.As(err, &response)
	return response, ok
}

func formatSlackErr(err error) string {
	if response, ok := slackError(err); ok && len(response.ResponseMetadata.Messages) > 0 {
		return fmt.Sprintf("%s (%s)", response.Err, strings.Join(response.ResponseMetadata.Messages, "; "))
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func copyIDs(ids []string) []string {
	return append([]string(nil), ids...)
}

func joinIDs(ids []string) string {
	cp := copyIDs(ids)
	sort.Strings(cp)
	return strings.Join(cp, ",")
}
