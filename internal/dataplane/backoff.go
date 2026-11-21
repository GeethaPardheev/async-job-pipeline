package dataplane

import (
	"math/rand/v2"
	"time"
)

const maxBackoffShift = 30

// Backoff computes retry delays: d = min(Cap, Base·2^(attempt−1)); delay = d + U[0, d).
type Backoff struct {
	Base time.Duration
	Cap  time.Duration
	// Int64N returns a uniform value in [0, n); nil uses math/rand/v2.
	Int64N func(n int64) int64
}

// Floor returns the deterministic part d for attempt (≥1; lower values are treated as 1).
func (b Backoff) Floor(attempt int) time.Duration {
	if b.Base <= 0 {
		return 0
	}
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}
	if b.Base > b.Cap>>shift {
		return b.Cap
	}
	return b.Base << shift
}

// Delay returns the jittered delay for attempt, in [Floor, 2·Floor).
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Floor(attempt)
	if d <= 0 {
		return 0
	}
	randN := b.Int64N
	if randN == nil {
		randN = rand.Int64N
	}
	return d + time.Duration(randN(int64(d)))
}

// jitter returns d ± 20% so N workers don't poll in lockstep.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := int64(d) * 2 / 5
	if spread <= 0 {
		return d
	}
	return time.Duration(int64(d) - spread/2 + rand.Int64N(spread))
}
