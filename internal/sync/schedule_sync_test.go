package sync

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PagerDuty/go-pagerduty"
	"github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindGroupPrefersActiveHandle(t *testing.T) {
	groups := []slack.UserGroup{
		{ID: "disabled", Handle: "oncall", Name: "Friendly", DateDelete: 5},
		{ID: "active", Handle: "oncall", Name: "Something else"},
		{ID: "by-name", Handle: "other", Name: "oncall"},
	}

	got, ok := findGroup(groups, "oncall")
	require.True(t, ok)
	assert.Equal(t, "active", got.ID)

	got, ok = findGroup([]slack.UserGroup{
		{ID: "disabled", Handle: "oncall", Name: "Friendly", DateDelete: 5},
	}, "ONCALL")
	require.True(t, ok)
	assert.Equal(t, "disabled", got.ID)

	got, ok = findGroup([]slack.UserGroup{
		{ID: "by-name", Handle: "other", Name: "oncall"},
	}, "oncall")
	require.True(t, ok)
	assert.Equal(t, "by-name", got.ID)
}

func TestSyncUsesListedMembersAndSkipsPerGroupReads(t *testing.T) {
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current Platform", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All Platforms", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.memberCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Equal(t, 0, slackAPI.createCalls)
	assert.Equal(t, 1, slackAPI.userCalls)
	assert.Equal(t, 2, slackAPI.groupCalls)
}

func TestSyncUpdatesWhenListedMembersDiffer(t *testing.T) {
	slackAPI := &fakeSlack{
		users: []slack.User{
			slackUser("U1", "a@b.c", false),
			slackUser("U2", "b@b.c", false),
		},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1", "U2"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"b@b.c", "a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.memberCalls)
	require.Equal(t, []string{"S1=U1,U2"}, slackAPI.updated)
	assert.Equal(t, 0, slackAPI.createCalls)
}

func TestSyncDoesNotRewriteSameMembersWhileListIsStale(t *testing.T) {
	slackAPI := &fakeSlack{
		stickyUsers: true,
		users:       []slack.User{slackUser("U2", "b@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U2"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"b@b.c"}}}
	syncer, advance, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 1, slackAPI.updateCalls)

	advance(slackMemberTTL + time.Minute)
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 2, slackAPI.updateCalls)
}

func TestRateLimitRetriesOnceThenPauses(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users:            []slack.User{slackUser("U1", "a@b.c", false)},
		memberRateLimits: 2,
		members:          map[string][]string{"S1": {"U1"}, "S2": {"U1"}},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current"},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All"},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, slept := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 2, slackAPI.memberCalls)
	assert.Equal(t, []time.Duration{30 * time.Second}, *slept)
	assert.Equal(t, 1, strings.Count(logs.String(), "slack rate limit reached"))
	assert.NotContains(t, logs.String(), "failed to update slack group")
}

func TestRateLimitRetrySucceedsWithoutErrorLog(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users:            []slack.User{slackUser("U1", "a@b.c", false)},
		memberRateLimits: 1,
		members:          map[string][]string{"S1": {"U1"}, "S2": {"U1"}},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current"},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All"},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, slept := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 3, slackAPI.memberCalls)
	assert.Equal(t, []time.Duration{30 * time.Second}, *slept)
	assert.NotContains(t, logs.String(), "slack rate limit reached")
	assert.Equal(t, 0, slackAPI.updateCalls)
}

func TestMissingSlackUserIsSkipped(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c", "github-ci-bot@whatnot.com"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Equal(t, 1, strings.Count(logs.String(), "github-ci-bot@whatnot.com"))
	assert.NotContains(t, logs.String(), "failed to update slack group")
}

func TestDeletedSlackUserIsSkipped(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users: []slack.User{
			slackUser("UDEL", "gone@b.c", true),
			slackUser("U1", "a@b.c", false),
		},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"gone@b.c", "a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Contains(t, logs.String(), "gone@b.c")
	assert.NotContains(t, logs.String(), "failed to update slack group")
}

func TestScheduleNotFoundIsSkippedUntilCooldown(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{users: []slack.User{slackUser("U1", "a@b.c", false)}}
	pd := &fakePD{errs: map[string]error{
		"PD1": pagerduty.APIError{StatusCode: 404},
	}}
	syncer, advance, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 1, pd.calls)
	assert.Equal(t, 1, strings.Count(logs.String(), "schedule PD1 was not found"))
	assert.Equal(t, 0, slackAPI.updateCalls)

	advance(missingScheduleCooldown + time.Minute)
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 2, pd.calls)
	assert.Equal(t, 1, strings.Count(logs.String(), "schedule PD1 was not found"))
}

func TestScheduleNotFoundStringFromOlderClient(t *testing.T) {
	slackAPI := &fakeSlack{}
	pd := &fakePD{errs: map[string]error{
		"PD1": fmt.Errorf("Failed call API endpoint. HTTP response code: 404 Error: &{3004 Schedule Not Found <nil>}"),
	}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 1, pd.calls)
}

func TestInvalidArgumentsIsNotRetried(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"UOLD"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}},
		},
		updateErr: slack.SlackErrorResponse{
			Err:              "invalid_arguments",
			ResponseMetadata: slack.ResponseMetadata{Messages: []string{"invalid user id"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())

	assert.Equal(t, 1, slackAPI.updateCalls)
	assert.Contains(t, logs.String(), "invalid_arguments (invalid user id)")
	assert.Equal(t, 1, strings.Count(logs.String(), "failed to update slack group current-oncall-platform"))
}

func TestEmptyOnCallDoesNotClearGroup(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Equal(t, 0, slackAPI.createCalls)
	assert.Equal(t, 2, strings.Count(logs.String(), "no slack users to sync"))
}

func TestUserDirectoryIsCached(t *testing.T) {
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, advance, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 1, slackAPI.userCalls)

	advance(slackUserTTL + time.Minute)
	require.NoError(t, syncer.Sync())
	assert.Equal(t, 2, slackAPI.userCalls)
}

func TestGroupFoundByHandleIsNotRecreated(t *testing.T) {
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current Platform Oncall", Users: []string{"U1"}},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All Platform Oncall", Users: []string{"U1"}},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	assert.Equal(t, 0, slackAPI.createCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
}

func TestHandleAlreadyExistsRefreshesGroupList(t *testing.T) {
	existing := []slack.UserGroup{
		{ID: "S1", Handle: "current-oncall-platform", Name: "Renamed Current", Users: []string{"U1"}},
		{ID: "S2", Handle: "all-oncall-platforms", Name: "Renamed All", Users: []string{"U1"}},
	}
	slackAPI := &fakeSlack{
		users:      []slack.User{slackUser("U1", "a@b.c", false)},
		groupQueue: [][]slack.UserGroup{{}, existing},
		createErr:  slack.SlackErrorResponse{Err: "handle_already_exists"},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 1, slackAPI.createCalls)
	assert.Equal(t, 2, slackAPI.groupCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
}

func TestDisabledGroupIsEnabled(t *testing.T) {
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}, DateDelete: 10},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}, DateDelete: 10},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())
	require.NoError(t, syncer.Sync())

	assert.Equal(t, 2, slackAPI.enableCalls)
	assert.Equal(t, 0, slackAPI.createCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
}

func TestUserGroupListFallsBackWhenMembersCannotBeIncluded(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users:           []slack.User{slackUser("U1", "a@b.c", false)},
		includeUsersErr: slack.SlackErrorResponse{Err: "invalid_arguments"},
		members:         map[string][]string{"S1": {"U1"}, "S2": {"U1"}},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current"},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All"},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 2, slackAPI.groupCalls)
	assert.Equal(t, 2, slackAPI.memberCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Contains(t, logs.String(), "listing groups without members")
}

func TestMemberReadBudgetDefersExtraLookups(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users:   []slack.User{slackUser("U1", "a@b.c", false)},
		members: map[string][]string{"S1": {"U1"}, "S2": {"U1"}},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current"},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All"},
		},
	}
	pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})
	syncer.maxMemberReads = 1

	require.NoError(t, syncer.Sync())
	assert.Equal(t, 1, slackAPI.memberCalls)
	assert.Contains(t, logs.String(), "deferred 1 slack membership lookups")

	require.NoError(t, syncer.Sync())
	assert.Equal(t, 2, slackAPI.memberCalls)
	assert.Equal(t, 0, slackAPI.updateCalls)
}

func TestPagerDutyErrorDoesNotUpdateSlack(t *testing.T) {
	logs := captureLogs(t)
	slackAPI := &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U9"}},
		},
	}
	pd := &fakePD{errs: map[string]error{"PD1": fmt.Errorf("pagerduty unavailable")}}
	syncer, _, _ := newTestSyncer(slackAPI, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, slackAPI.updateCalls)
	assert.Contains(t, logs.String(), "failed to get emails for current-oncall-platform")
	assert.Contains(t, logs.String(), "pagerduty unavailable")
}

func TestRateLimitDelayHonoursRetryAfter(t *testing.T) {
	delay, ok := rateLimitDelay(&slack.RateLimitedError{RetryAfter: 30 * time.Second})
	require.True(t, ok)
	assert.Equal(t, 30*time.Second, delay)

	delay, ok = rateLimitDelay(&slack.RateLimitedError{RetryAfter: 0})
	require.True(t, ok)
	assert.Equal(t, defaultRetryAfter, delay)

	delay, ok = rateLimitDelay(&slack.RateLimitedError{RetryAfter: 5 * time.Minute})
	require.True(t, ok)
	assert.Equal(t, maxRetryAfter, delay)

	_, ok = rateLimitDelay(fmt.Errorf("nope"))
	assert.False(t, ok)
}

type fakeSlack struct {
	groups           []slack.UserGroup
	groupQueue       [][]slack.UserGroup
	users            []slack.User
	members          map[string][]string
	stickyUsers      bool
	memberRateLimits int
	includeUsersErr  error
	createErr        error
	updateErr        error

	userCalls   int
	groupCalls  int
	memberCalls int
	updateCalls int
	createCalls int
	enableCalls int
	updated     []string
}

func (f *fakeSlack) GetUserGroupsContext(_ context.Context, options ...slack.GetUserGroupsOption) ([]slack.UserGroup, error) {
	f.groupCalls++
	params := &slack.GetUserGroupsParams{}
	for _, opt := range options {
		opt(params)
	}
	if params.IncludeUsers && f.includeUsersErr != nil {
		return nil, f.includeUsersErr
	}
	if len(f.groupQueue) > 0 {
		groups := f.groupQueue[0]
		f.groupQueue = f.groupQueue[1:]
		f.groups = groups
	}
	return cloneGroups(f.groups), nil
}

func (f *fakeSlack) GetUsersContext(context.Context, ...slack.GetUsersOption) ([]slack.User, error) {
	f.userCalls++
	return f.users, nil
}

func (f *fakeSlack) CreateUserGroupContext(_ context.Context, group slack.UserGroup, _ ...slack.CreateUserGroupOption) (slack.UserGroup, error) {
	f.createCalls++
	if f.createErr != nil {
		return slack.UserGroup{}, f.createErr
	}
	group.ID = "S" + group.Handle
	group.Users = []string{}
	f.groups = append(f.groups, group)
	return group, nil
}

func (f *fakeSlack) EnableUserGroupContext(_ context.Context, id string, _ ...slack.EnableUserGroupOption) (slack.UserGroup, error) {
	f.enableCalls++
	for i := range f.groups {
		if f.groups[i].ID == id {
			f.groups[i].DateDelete = 0
			return f.groups[i], nil
		}
	}
	return slack.UserGroup{ID: id}, nil
}

func (f *fakeSlack) GetUserGroupMembersContext(_ context.Context, id string, _ ...slack.GetUserGroupMembersOption) ([]string, error) {
	f.memberCalls++
	if f.memberRateLimits > 0 {
		f.memberRateLimits--
		return nil, &slack.RateLimitedError{RetryAfter: 30 * time.Second}
	}
	members, ok := f.members[id]
	if !ok {
		return []string{}, nil
	}
	return append([]string(nil), members...), nil
}

func (f *fakeSlack) UpdateUserGroupMembersContext(_ context.Context, id string, members string, _ ...slack.UpdateUserGroupMembersOption) (slack.UserGroup, error) {
	f.updateCalls++
	f.updated = append(f.updated, id+"="+members)
	if f.updateErr != nil {
		return slack.UserGroup{}, f.updateErr
	}
	if !f.stickyUsers {
		ids := []string{}
		if members != "" {
			ids = strings.Split(members, ",")
		}
		for i := range f.groups {
			if f.groups[i].ID == id {
				f.groups[i].Users = ids
			}
		}
	}
	return slack.UserGroup{ID: id}, nil
}

type fakePD struct {
	emails map[string][]string
	errs   map[string]error
	calls  int
}

func (f *fakePD) getEmailsForSchedule(id string, _ time.Duration) ([]string, error) {
	f.calls++
	if err, ok := f.errs[id]; ok {
		return nil, err
	}
	return append([]string(nil), f.emails[id]...), nil
}

func platformSchedule() Schedule {
	return Schedule{
		ScheduleIDs:            []string{"PD1"},
		CurrentOnCallGroupName: "current-oncall-platform",
		AllOnCallGroupName:     "all-oncall-platforms",
	}
}

func slackUser(id, email string, deleted bool) slack.User {
	return slack.User{
		ID:      id,
		Deleted: deleted,
		Profile: slack.UserProfile{Email: email},
	}
}

func newTestSyncer(slackAPI *fakeSlack, pd *fakePD, schedules []Schedule) (*Syncer, func(time.Duration), *[]time.Duration) {
	syncer := newSyncer(&Config{
		Schedules:                  schedules,
		PagerdutyScheduleLookahead: time.Hour,
	}, newSlackClient(slackAPI, nil, nil), pd)

	current := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	var slept []time.Duration
	now := func() time.Time { return current }
	syncer.now = now
	syncer.slack.now = now
	syncer.slack.sleep = func(d time.Duration) error {
		slept = append(slept, d)
		current = current.Add(d)
		return nil
	}
	advance := func(d time.Duration) {
		current = current.Add(d)
	}
	return syncer, advance, &slept
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	t.Cleanup(func() {
		logrus.SetOutput(os.Stderr)
	})
	return &buf
}

func cloneGroups(in []slack.UserGroup) []slack.UserGroup {
	out := make([]slack.UserGroup, len(in))
	for i := range in {
		out[i] = in[i]
		if in[i].Users != nil {
			out[i].Users = append([]string(nil), in[i].Users...)
		}
	}
	return out
}
