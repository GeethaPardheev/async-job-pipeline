package domain

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestParseStatus(t *testing.T) {
	for _, s := range AllStatuses {
		got, err := ParseStatus(string(s))
		if err != nil || got != s {
			t.Fatalf("ParseStatus(%q) = %q, %v", s, got, err)
		}
	}
	for _, bad := range []string{"", "dead_lettered", "QUEUED", "done"} {
		if _, err := ParseStatus(bad); err == nil {
			t.Fatalf("ParseStatus(%q) expected error", bad)
		}
	}
}

func TestIsTerminal(t *testing.T) {
	want := map[Status]bool{
		StatusQueued: false, StatusRunning: false,
		StatusSucceeded: true, StatusFailed: true, StatusDeadLettered: true, StatusCancelled: true,
	}
	for s, w := range want {
		if s.IsTerminal() != w {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, !w, w)
		}
	}
}

func TestTransitionTable(t *testing.T) {
	allowed := map[[2]Status]Transition{
		{StatusQueued, StatusRunning}:       TransitionClaim,
		{StatusRunning, StatusSucceeded}:    TransitionSucceed,
		{StatusRunning, StatusFailed}:       TransitionFail,
		{StatusRunning, StatusQueued}:       TransitionRetry,
		{StatusRunning, StatusDeadLettered}: TransitionDeadLetter,
		{StatusQueued, StatusCancelled}:     TransitionCancel,
	}
	for _, from := range AllStatuses {
		for _, to := range AllStatuses {
			want, ok := allowed[[2]Status{from, to}]
			if CanTransition(from, to) != ok {
				t.Errorf("CanTransition(%s,%s) = %v, want %v", from, to, !ok, ok)
			}
			got, err := TransitionFor(from, to)
			if ok && (err != nil || got != want) {
				t.Errorf("TransitionFor(%s,%s) = %q, %v; want %q", from, to, got, err, want)
			}
			if !ok && err == nil {
				t.Errorf("TransitionFor(%s,%s) expected error", from, to)
			}
		}
	}
}

func TestTerminalStatesHaveNoOutgoingEdges(t *testing.T) {
	for _, from := range AllStatuses {
		if !from.IsTerminal() {
			continue
		}
		for _, to := range AllStatuses {
			if CanTransition(from, to) {
				t.Errorf("terminal %s must not transition to %s", from, to)
			}
		}
	}
}

func TestNextStatus(t *testing.T) {
	tests := []struct {
		name       string
		o          Outcome
		attempt    int
		maxRetries int
		want       Status
	}{
		{"success first attempt", OutcomeSuccess, 1, 3, StatusSucceeded},
		{"permanent ignores budget", OutcomePermanent, 1, 3, StatusFailed},
		{"transient first of four", OutcomeTransient, 1, 3, StatusQueued},
		{"transient third of four", OutcomeTransient, 3, 3, StatusQueued},
		{"transient last of four", OutcomeTransient, 4, 3, StatusDeadLettered},
		{"zero retries dead-letters immediately", OutcomeTransient, 1, 0, StatusDeadLettered},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NextStatus(tt.o, tt.attempt, tt.maxRetries); got != tt.want {
				t.Fatalf("NextStatus = %s, want %s", got, tt.want)
			}
			if !CanTransition(StatusRunning, tt.want) {
				t.Fatalf("NextStatus produced %s, which is not reachable from running", tt.want)
			}
		})
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	p := Permanent(base)
	if !IsPermanent(p) || !errors.Is(p, base) {
		t.Fatalf("Permanent must match both ErrPermanent and the wrapped error")
	}
	if p.Error() != "bad payload" {
		t.Fatalf("Error() = %q", p.Error())
	}
	wrapped := fmt.Errorf("handler: %w", p)
	if !IsPermanent(wrapped) {
		t.Fatalf("permanence must survive further wrapping")
	}
	if IsPermanent(base) {
		t.Fatalf("plain error must not be permanent")
	}
	if Permanent(nil) != nil {
		t.Fatalf("Permanent(nil) must be nil")
	}
}

func TestParseCron(t *testing.T) {
	base := time.Date(2026, 11, 21, 10, 0, 30, 0, time.UTC)
	tests := []struct {
		expr string
		want time.Time
	}{
		{"*/5 * * * *", time.Date(2026, 11, 21, 10, 5, 0, 0, time.UTC)},
		{"0 * * * *", time.Date(2026, 11, 21, 11, 0, 0, 0, time.UTC)},
		{"@hourly", time.Date(2026, 11, 21, 11, 0, 0, 0, time.UTC)},
		{"@every 30s", base.Add(30 * time.Second)},
	}
	for _, tt := range tests {
		s, err := ParseCron(tt.expr)
		if err != nil {
			t.Fatalf("ParseCron(%q): %v", tt.expr, err)
		}
		if got := NextCronRun(s, base); !got.Equal(tt.want) {
			t.Errorf("NextCronRun(%q) = %s, want %s", tt.expr, got, tt.want)
		}
	}
	for _, bad := range []string{"", "* * *", "61 * * * *", "@every nope", "* * * * * *"} {
		if _, err := ParseCron(bad); err == nil {
			t.Errorf("ParseCron(%q) expected error", bad)
		}
	}
}
