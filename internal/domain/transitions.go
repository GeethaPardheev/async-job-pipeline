package domain

import "fmt"

// Transition identifies an allowed edge of the job state machine (SPEC §4.3).
type Transition string

const (
	TransitionClaim      Transition = "T2_claim"
	TransitionSucceed    Transition = "T3_succeed"
	TransitionFail       Transition = "T4_fail"
	TransitionRetry      Transition = "T5_retry"
	TransitionDeadLetter Transition = "T6_dead_letter"
	TransitionCancel     Transition = "T7_cancel"
)

type edge struct{ from, to Status }

var transitions = map[edge]Transition{
	{StatusQueued, StatusRunning}:       TransitionClaim,
	{StatusRunning, StatusSucceeded}:    TransitionSucceed,
	{StatusRunning, StatusFailed}:       TransitionFail,
	{StatusRunning, StatusQueued}:       TransitionRetry,
	{StatusRunning, StatusDeadLettered}: TransitionDeadLetter,
	{StatusQueued, StatusCancelled}:     TransitionCancel,
}

// CanTransition reports whether from → to is an edge of the state machine.
func CanTransition(from, to Status) bool {
	_, ok := transitions[edge{from, to}]
	return ok
}

// TransitionFor returns the named transition for from → to, or an error if it is forbidden.
func TransitionFor(from, to Status) (Transition, error) {
	t, ok := transitions[edge{from, to}]
	if !ok {
		return "", fmt.Errorf("forbidden transition %s -> %s", from, to)
	}
	return t, nil
}

// Outcome classifies how an attempt ended, before retry budget is considered.
type Outcome int

const (
	OutcomeSuccess Outcome = iota
	OutcomePermanent
	OutcomeTransient
)

// NextStatus decides the post-attempt status. attemptCount is the attempt that just finished (≥1);
// total attempts allowed = 1 + maxRetries.
func NextStatus(o Outcome, attemptCount, maxRetries int) Status {
	switch o {
	case OutcomeSuccess:
		return StatusSucceeded
	case OutcomePermanent:
		return StatusFailed
	default:
		if attemptCount <= maxRetries {
			return StatusQueued
		}
		return StatusDeadLettered
	}
}
