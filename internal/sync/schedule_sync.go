package sync

import (
	"errors"
	"strings"
	"time"

	"github.com/PagerDuty/go-pagerduty"
	"github.com/kevholditch/go-pagerduty-slack-sync/internal/compare"
	"github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
)

const (
	slackMemberTTL          = 10 * time.Minute
	missingScheduleCooldown = 30 * time.Minute
)

type scheduleLister interface {
	getEmailsForSchedule(id string, lookahead time.Duration) ([]string, error)
}

type memberState struct {
	ids         []string
	at          time.Time
	rejected    bool
	rejectedKey string
}

// Syncer syncs PagerDuty schedules into Slack usergroups.
// Reuse one Syncer across runs so the Slack user cache and backoff survive.
type Syncer struct {
	config *Config
	slack  *slackClient
	pd     scheduleLister
	now    func() time.Time

	members           map[string]memberState
	skipScheduleUntil map[string]time.Time
	warnedSchedule    map[string]struct{}
	warnedEmail       map[string]struct{}
	warnedEmpty       map[string]struct{}

	maxMemberReads   int
	memberTTL        time.Duration
	scheduleCooldown time.Duration

	reads           int
	deferred        int
	rateLimitLogged bool
}

// NewSyncer builds a Syncer that talks to Slack and PagerDuty.
func NewSyncer(config *Config) *Syncer {
	return newSyncer(
		config,
		newSlackClient(slack.New(config.SlackToken), time.Now, nil),
		newPagerDutyClient(config.PagerDutyToken),
	)
}

func newSyncer(config *Config, slackClient *slackClient, pd scheduleLister) *Syncer {
	return &Syncer{
		config:            config,
		slack:             slackClient,
		pd:                pd,
		now:               time.Now,
		members:           map[string]memberState{},
		skipScheduleUntil: map[string]time.Time{},
		warnedSchedule:    map[string]struct{}{},
		warnedEmail:       map[string]struct{}{},
		warnedEmpty:       map[string]struct{}{},
		maxMemberReads:    maxMemberReadsPerCycle,
		memberTTL:         slackMemberTTL,
		scheduleCooldown:  missingScheduleCooldown,
	}
}

// Schedules syncs once. Callers that loop should keep a Syncer instead.
func Schedules(config *Config) error {
	return NewSyncer(config).Sync()
}

// Sync runs one pass over the configured schedules.
func (s *Syncer) Sync() error {
	logrus.Infof("running schedule sync...")
	s.slack.beginCycle()
	s.reads = 0
	s.deferred = 0
	s.rateLimitLogged = false

	if err := s.ignoreRateLimit(s.slack.ensureUsers()); err != nil {
		return err
	}
	if err := s.ignoreRateLimit(s.slack.ensureGroups()); err != nil {
		return err
	}

	for _, schedule := range s.config.Schedules {
		s.syncGroup(schedule.CurrentOnCallGroupName, schedule.ScheduleIDs, time.Second)
		s.syncGroup(schedule.AllOnCallGroupName, schedule.ScheduleIDs, s.config.PagerdutyScheduleLookahead)
	}
	if s.deferred > 0 {
		logrus.Infof("deferred %d slack membership lookups to stay under the usergroups.users.list rate limit", s.deferred)
	}
	return nil
}

func (s *Syncer) syncGroup(name string, scheduleIDs []string, lookahead time.Duration) {
	if s.slack.blocked {
		s.noteRateLimit(s.slack.blockErr)
		return
	}
	logrus.Infof("checking slack group: %s", name)

	emails, err := s.emailsFor(scheduleIDs, lookahead)
	if err != nil {
		logrus.Errorf("failed to get emails for %s: %v", name, err)
		return
	}
	ids, missing := s.slack.idsForEmails(emails)
	for _, email := range missing {
		warnOnce(s.warnedEmail, email, "skipping pagerduty user with no active slack account: %s", email)
	}
	if len(ids) == 0 {
		if len(emails) == 0 && !s.allSchedulesSkipped(scheduleIDs) {
			warnOnce(s.warnedEmpty, name, "skipping slack group %s: no slack users to sync", name)
		}
		return
	}
	if err := s.applyGroup(name, ids); err != nil && s.ignoreRateLimit(err) != nil {
		logrus.Errorf("failed to update slack group %s: %v", name, formatSlackErr(err))
	}
}

func (s *Syncer) applyGroup(name string, desired []string) error {
	group, err := s.slack.createOrGet(name)
	if err != nil {
		return err
	}
	if group.DateDelete != 0 {
		group, err = s.slack.enable(group)
		if err != nil {
			return err
		}
	}

	key := joinIDs(desired)
	state, seen := s.members[name]
	if seen && state.rejected && state.rejectedKey == key {
		return nil
	}

	// A write can be newer than usergroups.list. Skip another write of the
	// same set until memberTTL when the list is missing or still stale.
	if seen && !state.rejected && sameMembers(state.ids, desired) && s.now().Sub(state.at) < s.memberTTL {
		if group.Users == nil || !sameMembers(group.Users, desired) {
			return nil
		}
	}

	if group.Users != nil && sameMembers(group.Users, desired) {
		s.remember(name, desired, false)
		return nil
	}

	if group.Users == nil {
		if s.reads >= s.maxMemberReads {
			s.deferred++
			return nil
		}
		s.reads++
		current, err := s.slack.listMembers(group.ID)
		if err != nil {
			return err
		}
		if sameMembers(current, desired) {
			s.remember(name, desired, false)
			return nil
		}
	}

	logrus.Infof("member list %s needs updating...", name)
	if err := s.slack.updateMembers(group.ID, desired); err != nil {
		if isInvalidArguments(err) {
			// The same payload will fail again. Wait until membership changes.
			s.remember(name, desired, true)
		}
		return err
	}
	s.remember(name, desired, false)
	s.slack.setMembers(group.ID, desired)
	return nil
}

func (s *Syncer) remember(name string, ids []string, rejected bool) {
	state := memberState{ids: copyIDs(ids), at: s.now()}
	if rejected {
		state.rejected = true
		state.rejectedKey = joinIDs(ids)
	}
	s.members[name] = state
}

func (s *Syncer) emailsFor(ids []string, lookahead time.Duration) ([]string, error) {
	var emails []string
	for _, id := range ids {
		if until, ok := s.skipScheduleUntil[id]; ok && s.now().Before(until) {
			continue
		}
		found, err := s.pd.getEmailsForSchedule(id, lookahead)
		if err != nil {
			if isScheduleNotFound(err) {
				s.skipScheduleUntil[id] = s.now().Add(s.scheduleCooldown)
				warnOnce(s.warnedSchedule, id, "pagerduty schedule %s was not found; skipping it for %s", id, s.scheduleCooldown)
				continue
			}
			return nil, err
		}
		delete(s.skipScheduleUntil, id)
		emails = appendIfMissing(emails, found...)
	}
	return emails, nil
}

func (s *Syncer) allSchedulesSkipped(ids []string) bool {
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		until, ok := s.skipScheduleUntil[id]
		if !ok || !s.now().Before(until) {
			return false
		}
	}
	return true
}

func warnOnce(seen map[string]struct{}, key, format string, args ...any) {
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	logrus.Warnf(format, args...)
}

func (s *Syncer) ignoreRateLimit(err error) error {
	if !isRateLimited(err) {
		return err
	}
	s.noteRateLimit(err)
	return nil
}

func (s *Syncer) noteRateLimit(err error) {
	if s.rateLimitLogged || err == nil {
		return
	}
	s.rateLimitLogged = true
	logrus.Errorf("slack rate limit reached: %v", err)
}

func sameMembers(a, b []string) bool {
	return compare.Array(copyIDs(a), copyIDs(b))
}

func isScheduleNotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr pagerduty.APIError
	if errors.As(err, &apiErr) {
		if apiErr.NotFound() || (apiErr.APIError.Valid && apiErr.APIError.ErrorObject.Code == 3004) {
			return true
		}
	}
	return strings.Contains(err.Error(), "Schedule Not Found")
}

func appendIfMissing(slice []string, items ...string) []string {
out:
	for _, i := range items {
		for _, ele := range slice {
			if ele == i {
				continue out
			}
		}
		slice = append(slice, i)
	}

	return slice
}
