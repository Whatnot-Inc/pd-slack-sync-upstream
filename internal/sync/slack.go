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
	if err := c.wait(); err != nil {
		return err
	}
	err := call()
	delay, limited := rateLimitDelay(err)
	if !limited {
		return err
	}
	c.limitedUntil = c.now().Add(delay)
	if err = c.wait(); err != nil {
		return err
	}
	err = call()
	if delay, limited = rateLimitDelay(err); limited {
		c.limitedUntil = c.now().Add(delay)
		c.blocked = true
		c.blockErr = err
	}
	return err
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
	users, err := c.api.GetUsersContext(context.Background())
	if err != nil {
		if len(c.users) > 0 {
			logrus.Warnf("using cached slack users after refresh failed: %v", formatSlackErr(err))
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
		var fallbackErr error
		groups, fallbackErr = c.listGroups(false)
		if fallbackErr != nil {
			err = fallbackErr
		} else {
			err = nil
		}
	}
	if err != nil {
		if len(c.userGroups) > 0 && isRateLimited(err) {
			logrus.Warnf("using cached slack usergroups after refresh failed: %v", formatSlackErr(err))
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
	var groups []slack.UserGroup
	err := c.do(func() error {
		options := []slack.GetUserGroupsOption{
			slack.GetUserGroupsOptionIncludeDisabled(true),
		}
		if includeUsers {
			options = append(options, slack.GetUserGroupsOptionIncludeUsers(true))
		}
		var callErr error
		groups, callErr = c.api.GetUserGroupsContext(context.Background(), options...)
		return callErr
	})
	return groups, err
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

	var created slack.UserGroup
	err := c.do(func() error {
		var callErr error
		created, callErr = c.api.CreateUserGroupContext(context.Background(), slack.UserGroup{
			Name:   name,
			Handle: name,
		})
		return callErr
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
	var enabled slack.UserGroup
	err := c.do(func() error {
		var callErr error
		enabled, callErr = c.api.EnableUserGroupContext(context.Background(), group.ID)
		return callErr
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
	var members []string
	err := c.do(func() error {
		var callErr error
		members, callErr = c.api.GetUserGroupMembersContext(context.Background(), id)
		return callErr
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
	cp := append([]string(nil), ids...)
	for i := range c.userGroups {
		if c.userGroups[i].ID == id {
			c.userGroups[i].Users = cp
			return
		}
	}
}

func (c *slackClient) putGroup(group slack.UserGroup) {
	for i := range c.userGroups {
		if c.userGroups[i].ID == group.ID {
			if group.Users == nil {
				group.Users = c.userGroups[i].Users
			}
			c.userGroups[i] = group
			return
		}
	}
	c.userGroups = append(c.userGroups, group)
}

// findGroup prefers an active handle match. Display names often differ from
// the handle this service created, and a disabled group is absent from
// usergroups.list unless include_disabled is set.
func findGroup(groups []slack.UserGroup, name string) (slack.UserGroup, bool) {
	var handleDisabled, nameActive, nameDisabled *slack.UserGroup
	for i := range groups {
		group := groups[i]
		handleMatch := group.Handle != "" && strings.EqualFold(group.Handle, name)
		nameMatch := strings.EqualFold(group.Name, name)
		disabled := group.DateDelete != 0
		if handleMatch && !disabled {
			return group, true
		}
		if handleMatch && disabled && handleDisabled == nil {
			cp := group
			handleDisabled = &cp
		}
		if nameMatch && !disabled && nameActive == nil {
			cp := group
			nameActive = &cp
		}
		if nameMatch && disabled && nameDisabled == nil {
			cp := group
			nameDisabled = &cp
		}
	}
	for _, group := range []*slack.UserGroup{handleDisabled, nameActive, nameDisabled} {
		if group != nil {
			return *group, true
		}
	}
	return slack.UserGroup{}, false
}

func isRateLimited(err error) bool {
	_, ok := rateLimitDelay(err)
	return ok
}

func isAlreadyExists(err error) bool {
	code := slackErrorCode(err)
	return code == "handle_already_exists" || code == "name_already_exists" || code == "already_exists"
}

func isInvalidArguments(err error) bool {
	return slackErrorCode(err) == "invalid_arguments"
}

func slackErrorCode(err error) string {
	var response slack.SlackErrorResponse
	if errors.As(err, &response) {
		return response.Err
	}
	return ""
}

func formatSlackErr(err error) string {
	var response slack.SlackErrorResponse
	if errors.As(err, &response) && len(response.ResponseMetadata.Messages) > 0 {
		return fmt.Sprintf("%s (%s)", response.Err, strings.Join(response.ResponseMetadata.Messages, "; "))
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func joinIDs(ids []string) string {
	cp := append([]string(nil), ids...)
	sort.Strings(cp)
	return strings.Join(cp, ",")
}
