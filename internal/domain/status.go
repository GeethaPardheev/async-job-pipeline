// Package domain holds the pure job model shared by the control plane and data plane.
package domain

import "fmt"

// Status is a job lifecycle state. Values must match the jobs.status CHECK constraint.
type Status string

const (
	StatusQueued       Status = "queued"
	StatusRunning      Status = "running"
	StatusSucceeded    Status = "succeeded"
	StatusFailed       Status = "failed"
	StatusDeadLettered Status = "dead-lettered"
	StatusCancelled    Status = "cancelled"
)

// AllStatuses lists every status in lifecycle order.
var AllStatuses = []Status{
	StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusDeadLettered, StatusCancelled,
}

// ParseStatus validates s against the status enum.
func ParseStatus(s string) (Status, error) {
	for _, st := range AllStatuses {
		if string(st) == s {
			return st, nil
		}
	}
	return "", fmt.Errorf("invalid status %q", s)
}

// IsTerminal reports whether no further transition is allowed out of s.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusDeadLettered, StatusCancelled:
		return true
	default:
		return false
	}
}

// AttemptStatus is the outcome of one execution attempt. Values must match the job_attempts.status CHECK constraint.
type AttemptStatus string

const (
	AttemptRunning      AttemptStatus = "running"
	AttemptSucceeded    AttemptStatus = "succeeded"
	AttemptFailed       AttemptStatus = "failed"
	AttemptTimedOut     AttemptStatus = "timed_out"
	AttemptLeaseExpired AttemptStatus = "lease_expired"
	AttemptFenced       AttemptStatus = "fenced"
)

// AllAttemptStatuses lists every attempt status.
var AllAttemptStatuses = []AttemptStatus{
	AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptLeaseExpired, AttemptFenced,
}
