package schedule

import (
	"testing"
	"time"
)

func TestIntervals(t *testing.T) {
	for spec, want := range map[string]time.Duration{
		"5 minutes": 5 * time.Minute, "5 MINUTE": 5 * time.Minute, "1 hour": time.Hour, "every 2 days": 48 * time.Hour,
		"30 seconds": 30 * time.Second, "90s": 90 * time.Second, "1h30m": 90 * time.Minute, " 10   min ": 10 * time.Minute,
	} {
		s, err := Parse(spec, "")
		if err != nil {
			t.Errorf("%q: %v", spec, err)
			continue
		}
		if !s.IsInterval() || s.Every != want {
			t.Errorf("%q: got %v, want every %v", spec, s.Every, want)
		}
	}
	for _, spec := range []string{"", "0 minutes", "-1 hour", "500ms", "5 fortnights", "five minutes", "1 2 3"} {
		if _, err := Parse(spec, ""); err == nil {
			t.Errorf("%q: parsed", spec)
		}
	}
}

func TestIntervalNextSkipsMissedRuns(t *testing.T) {
	s, _ := Parse("10 minutes", "")
	prev := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	if got := s.Next(prev, prev.Add(time.Second)); !got.Equal(prev.Add(10 * time.Minute)) {
		t.Errorf("next: %v", got)
	}
	// Down for 35 minutes: the runs at 9:10, 9:20 and 9:30 are skipped,
	// and the grid stays on the tens.
	if got := s.Next(prev, prev.Add(35*time.Minute)); !got.Equal(prev.Add(40 * time.Minute)) {
		t.Errorf("after a gap: %v", got)
	}
	if got := s.Next(prev, prev.Add(30*time.Minute)); !got.Equal(prev.Add(40 * time.Minute)) {
		t.Errorf("exactly on a run: %v", got)
	}
	now := time.Date(2026, 10, 16, 9, 3, 0, 0, time.UTC)
	if got := s.First(now); !got.Equal(now.Add(10 * time.Minute)) {
		t.Errorf("first: %v", got)
	}
}

func TestCron(t *testing.T) {
	at := func(s string) time.Time {
		t.Helper()
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range []struct {
		spec, tz, after, want string
	}{
		{"0 6 * * *", "", "2026-10-16T05:59:00Z", "2026-10-16T06:00:00Z"},
		{"0 6 * * *", "", "2026-10-16T06:00:00Z", "2026-10-17T06:00:00Z"},
		{"*/15 * * * *", "", "2026-10-16T06:07:31Z", "2026-10-16T06:15:00Z"},
		{"5/20 9-10 * * *", "", "2026-10-16T09:50:00Z", "2026-10-16T10:05:00Z"},
		{"0 9 * * MON-FRI", "", "2026-10-16T10:00:00Z", "2026-10-19T09:00:00Z"}, // Friday after 9: Monday
		{"0 0 1 JAN,jul *", "", "2026-02-01T00:00:00Z", "2026-07-01T00:00:00Z"},
		{"0 0 29 2 *", "", "2026-03-01T00:00:00Z", "2028-02-29T00:00:00Z"},
		{"0 0 * * 7", "", "2026-10-16T00:00:00Z", "2026-10-18T00:00:00Z"}, // 7 is Sunday
		// Day of month and day of week both restricted: either matches.
		{"0 0 13 * 5", "", "2026-10-10T00:00:00Z", "2026-10-13T00:00:00Z"},
		{"@hourly", "", "2026-10-16T06:30:00Z", "2026-10-16T07:00:00Z"},
		{"@daily", "", "2026-10-16T06:30:00Z", "2026-10-17T00:00:00Z"},
		// 6 a.m. in New York is 10:00 UTC in summer time and 11:00 after it.
		{"0 6 * * *", "America/New_York", "2026-10-16T12:00:00Z", "2026-10-17T10:00:00Z"},
		{"0 6 * * *", "America/New_York", "2026-11-01T12:00:00Z", "2026-11-02T11:00:00Z"},
		{"USING CRON 0 6 * * * America/New_York", "", "2026-10-16T12:00:00Z", "2026-10-17T10:00:00Z"},
		// 2:30 a.m. does not exist on the day summer time starts; the run
		// moves to the next time that matches.
		{"30 2 * * *", "America/New_York", "2026-03-08T05:00:00Z", "2026-03-09T06:30:00Z"},
	} {
		s, err := Parse(c.spec, c.tz)
		if err != nil {
			t.Errorf("%s: %v", c.spec, err)
			continue
		}
		if s.IsInterval() {
			t.Errorf("%s: parsed as an interval", c.spec)
		}
		if got := s.Next(at(c.after), at(c.after)); !got.Equal(at(c.want)) {
			t.Errorf("%s in %q after %s: got %s, want %s", c.spec, c.tz, c.after, got.UTC().Format(time.RFC3339), c.want)
		}
	}

	for _, spec := range []string{"* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "* * * * 8",
		"5-1 * * * *", "*/0 * * * *", "x * * * *"} {
		if _, err := Parse(spec, ""); err == nil {
			t.Errorf("%q: parsed", spec)
		}
	}
	if _, err := Parse("0 6 * * *", "Mars/Olympus"); err == nil {
		t.Error("unknown time zone accepted")
	}
	if _, err := Parse("USING CRON 0 6 * * * UTC", "Europe/Paris"); err == nil {
		t.Error("two time zones accepted")
	}
	s, _ := Parse("0 0 31 2 *", "")
	if !Never(s.Next(at("2026-01-01T00:00:00Z"), at("2026-01-01T00:00:00Z"))) {
		t.Error("February 31st matched")
	}
}
