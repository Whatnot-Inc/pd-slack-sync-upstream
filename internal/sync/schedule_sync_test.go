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

func TestSyncUpdatesMembersWhenTheyChange(t *testing.T) {
	api := &fakeSlack{
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
	syncer, _, _ := newTestSyncer(api, pd, []Schedule{platformSchedule()})

	require.NoError(t, syncer.Sync())

	assert.Equal(t, 0, api.memberCalls)
	assert.Equal(t, 0, api.createCalls)
	require.Equal(t, []string{"S1=U1,U2"}, api.updated)
}

func TestSlackRateLimit(t *testing.T) {
	tests := []struct {
		name        string
		limits      int
		memberCalls int
		updateCalls int
		errorLog    bool
	}{
		{name: "retries once then pauses the rest of the run", limits: 2, memberCalls: 2, errorLog: true},
		{name: "one retry succeeds", limits: 1, memberCalls: 3, updateCalls: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureLogs(t)
			api := &fakeSlack{
				users:            []slack.User{slackUser("U1", "a@b.c", false)},
				memberRateLimits: tt.limits,
				members:          map[string][]string{"S1": {"U1"}, "S2": {"U1"}},
				groups: []slack.UserGroup{
					{ID: "S1", Handle: "current-oncall-platform", Name: "Current"},
					{ID: "S2", Handle: "all-oncall-platforms", Name: "All"},
				},
			}
			pd := &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}
			syncer, _, slept := newTestSyncer(api, pd, []Schedule{platformSchedule()})

			require.NoError(t, syncer.Sync())

			assert.Equal(t, tt.memberCalls, api.memberCalls)
			assert.Equal(t, tt.updateCalls, api.updateCalls)
			assert.Equal(t, []time.Duration{30 * time.Second}, *slept)
			assert.Equal(t, tt.errorLog, strings.Contains(logs.String(), "slack rate limit reached"))
			assert.NotContains(t, logs.String(), "failed to update slack group")
		})
	}
}

func TestRateLimitDelay(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		delay time.Duration
		ok    bool
	}{
		{name: "retry after", err: &slack.RateLimitedError{RetryAfter: 30 * time.Second}, delay: 30 * time.Second, ok: true},
		{name: "missing retry after", err: &slack.RateLimitedError{RetryAfter: 0}, delay: defaultRetryAfter, ok: true},
		{name: "capped retry after", err: &slack.RateLimitedError{RetryAfter: 5 * time.Minute}, delay: maxRetryAfter, ok: true},
		{name: "other error", err: fmt.Errorf("nope")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay, ok := rateLimitDelay(tt.err)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.delay, delay)
		})
	}
}

func TestGroupHandleMatching(t *testing.T) {
	t.Run("handle", func(t *testing.T) {
		api := listedGroups(false)
		api.groups[0].Name = "Current Platform Oncall"
		api.groups[1].Name = "All Platform Oncall"
		syncListed(t, api, []string{"a@b.c"})
		assert.Equal(t, 0, api.createCalls)
		assert.Equal(t, 0, api.updateCalls)
		assert.Equal(t, 0, api.enableCalls)
	})

	t.Run("disabled", func(t *testing.T) {
		api := listedGroups(true)
		syncer, _, _ := newTestSyncer(api, &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}, []Schedule{platformSchedule()})
		require.NoError(t, syncer.Sync())
		require.NoError(t, syncer.Sync())
		assert.Equal(t, 2, api.enableCalls)
		assert.Equal(t, 0, api.createCalls)
		assert.Equal(t, 0, api.updateCalls)
	})

	t.Run("already exists", func(t *testing.T) {
		existing := listedGroups(false).groups
		api := &fakeSlack{
			users:      []slack.User{slackUser("U1", "a@b.c", false)},
			groupQueue: [][]slack.UserGroup{{}, existing},
			createErr:  slack.SlackErrorResponse{Err: "handle_already_exists"},
		}
		syncListed(t, api, []string{"a@b.c"})
		assert.Equal(t, 1, api.createCalls)
		assert.Equal(t, 2, api.groupCalls)
		assert.Equal(t, 0, api.updateCalls)
	})
}

func TestSkipsMissingSlackUsers(t *testing.T) {
	tests := []struct {
		name   string
		users  []slack.User
		emails []string
		warn   string
	}{
		{
			name:   "unknown email",
			users:  []slack.User{slackUser("U1", "a@b.c", false)},
			emails: []string{"a@b.c", "github-ci-bot@whatnot.com"},
			warn:   "github-ci-bot@whatnot.com",
		},
		{
			name: "deleted user",
			users: []slack.User{
				slackUser("UDEL", "gone@b.c", true),
				slackUser("U1", "a@b.c", false),
			},
			emails: []string{"gone@b.c", "a@b.c"},
			warn:   "gone@b.c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureLogs(t)
			api := listedGroups(false)
			api.users = tt.users
			pd := &fakePD{emails: map[string][]string{"PD1": tt.emails}}
			syncer, _, _ := newTestSyncer(api, pd, []Schedule{platformSchedule()})

			require.NoError(t, syncer.Sync())
			require.NoError(t, syncer.Sync())

			assert.Equal(t, 0, api.updateCalls)
			assert.Equal(t, 1, strings.Count(logs.String(), tt.warn))
			assert.NotContains(t, logs.String(), "failed to update slack group")
		})
	}
}

func TestDoesNotResendEmptyOrRejectedPayloads(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		logs := captureLogs(t)
		api := listedGroups(false)
		syncer, _, _ := newTestSyncer(api, &fakePD{emails: map[string][]string{"PD1": {}}}, []Schedule{platformSchedule()})

		require.NoError(t, syncer.Sync())
		require.NoError(t, syncer.Sync())

		assert.Equal(t, 0, api.updateCalls)
		assert.Equal(t, 0, api.createCalls)
		assert.Equal(t, 2, strings.Count(logs.String(), "no slack users to sync"))
	})

	t.Run("rejected", func(t *testing.T) {
		logs := captureLogs(t)
		api := listedGroups(false)
		api.groups[0].Users = []string{"UOLD"}
		api.updateErr = slack.SlackErrorResponse{
			Err:              "invalid_arguments",
			ResponseMetadata: slack.ResponseMetadata{Messages: []string{"invalid user id"}},
		}
		syncer, _, _ := newTestSyncer(api, &fakePD{emails: map[string][]string{"PD1": {"a@b.c"}}}, []Schedule{platformSchedule()})

		require.NoError(t, syncer.Sync())
		require.NoError(t, syncer.Sync())

		assert.Equal(t, 1, api.updateCalls)
		assert.Contains(t, logs.String(), "invalid_arguments (invalid user id)")
		assert.Equal(t, 1, strings.Count(logs.String(), "failed to update slack group current-oncall-platform"))
	})
}

func TestSkipsMissingPagerDutySchedule(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "404", err: pagerduty.APIError{StatusCode: 404}},
		{name: "older client", err: fmt.Errorf("Failed call API endpoint. HTTP response code: 404 Error: &{3004 Schedule Not Found <nil>}")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureLogs(t)
			api := &fakeSlack{users: []slack.User{slackUser("U1", "a@b.c", false)}}
			pd := &fakePD{errs: map[string]error{"PD1": tt.err}}
			syncer, advance, _ := newTestSyncer(api, pd, []Schedule{platformSchedule()})

			require.NoError(t, syncer.Sync())
			require.NoError(t, syncer.Sync())
			assert.Equal(t, 1, pd.calls)
			assert.Equal(t, 0, api.updateCalls)
			assert.Equal(t, 1, strings.Count(logs.String(), "schedule PD1 was not found"))

			advance(missingScheduleCooldown + time.Minute)
			require.NoError(t, syncer.Sync())
			assert.Equal(t, 2, pd.calls)
			assert.Equal(t, 1, strings.Count(logs.String(), "schedule PD1 was not found"))
		})
	}
}

func listedGroups(disabled bool) *fakeSlack {
	var deleted slack.JSONTime
	if disabled {
		deleted = 10
	}
	return &fakeSlack{
		users: []slack.User{slackUser("U1", "a@b.c", false)},
		groups: []slack.UserGroup{
			{ID: "S1", Handle: "current-oncall-platform", Name: "Current", Users: []string{"U1"}, DateDelete: deleted},
			{ID: "S2", Handle: "all-oncall-platforms", Name: "All", Users: []string{"U1"}, DateDelete: deleted},
		},
	}
}

func syncListed(t *testing.T, api *fakeSlack, emails []string) {
	t.Helper()
	syncer, _, _ := newTestSyncer(api, &fakePD{emails: map[string][]string{"PD1": emails}}, []Schedule{platformSchedule()})
	require.NoError(t, syncer.Sync())
}

type fakeSlack struct {
	groups           []slack.UserGroup
	groupQueue       [][]slack.UserGroup
	users            []slack.User
	members          map[string][]string
	memberRateLimits int
	createErr        error
	updateErr        error

	groupCalls  int
	memberCalls int
	updateCalls int
	createCalls int
	enableCalls int
	updated     []string
}

func (f *fakeSlack) GetUserGroupsContext(context.Context, ...slack.GetUserGroupsOption) ([]slack.UserGroup, error) {
	f.groupCalls++
	if len(f.groupQueue) > 0 {
		groups := f.groupQueue[0]
		f.groupQueue = f.groupQueue[1:]
		f.groups = groups
	}
	return cloneGroups(f.groups), nil
}

func (f *fakeSlack) GetUsersContext(context.Context, ...slack.GetUsersOption) ([]slack.User, error) {
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
	ids := []string{}
	if members != "" {
		ids = strings.Split(members, ",")
	}
	for i := range f.groups {
		if f.groups[i].ID == id {
			f.groups[i].Users = ids
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

func newTestSyncer(api *fakeSlack, pd *fakePD, schedules []Schedule) (*Syncer, func(time.Duration), *[]time.Duration) {
	syncer := newSyncer(&Config{
		Schedules:                  schedules,
		PagerdutyScheduleLookahead: time.Hour,
	}, newSlackClient(api, nil, nil), pd)

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
	return syncer, func(d time.Duration) { current = current.Add(d) }, &slept
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	t.Cleanup(func() { logrus.SetOutput(os.Stderr) })
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
