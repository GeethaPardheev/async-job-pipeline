package dataplane

import (
	"testing"
	"time"
)

func TestBackoffFloor(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 5 * time.Minute}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, {1, time.Second}, {2, 2 * time.Second}, {3, 4 * time.Second},
		{9, 256 * time.Second}, {10, 5 * time.Minute}, {31, 5 * time.Minute}, {1000, 5 * time.Minute},
	}
	for _, tt := range tests {
		if got := b.Floor(tt.attempt); got != tt.want {
			t.Errorf("Floor(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffFloorNoOverflow(t *testing.T) {
	b := Backoff{Base: time.Hour, Cap: 1<<62 - 1}
	for attempt := 1; attempt < 100; attempt++ {
		if d := b.Floor(attempt); d <= 0 || d > b.Cap {
			t.Fatalf("Floor(%d) = %d out of (0, cap]", attempt, d)
		}
	}
}

func TestBackoffDelayExactWithSeededRand(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Cap: time.Second, Int64N: func(n int64) int64 { return n / 2 }}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 150 * time.Millisecond},  // d=100ms + 50ms
		{2, 300 * time.Millisecond},  // d=200ms + 100ms
		{4, 1200 * time.Millisecond}, // d=800ms + 400ms
		{5, 1500 * time.Millisecond}, // d=cap 1s + 500ms
	}
	for _, tt := range tests {
		if got := b.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffDelayExtremes(t *testing.T) {
	low := Backoff{Base: time.Second, Cap: time.Minute, Int64N: func(int64) int64 { return 0 }}
	high := Backoff{Base: time.Second, Cap: time.Minute, Int64N: func(n int64) int64 { return n - 1 }}
	for attempt := 1; attempt <= 10; attempt++ {
		d := low.Floor(attempt)
		if got := low.Delay(attempt); got != d {
			t.Errorf("min jitter Delay(%d) = %s, want floor %s", attempt, got, d)
		}
		if got := high.Delay(attempt); got != 2*d-1 {
			t.Errorf("max jitter Delay(%d) = %s, want %s", attempt, got, 2*d-1)
		}
	}
}

func TestBackoffDelayRandomBounds(t *testing.T) {
	b := Backoff{Base: 50 * time.Millisecond, Cap: 2 * time.Second}
	for attempt := 1; attempt <= 12; attempt++ {
		d := b.Floor(attempt)
		for i := 0; i < 200; i++ {
			if got := b.Delay(attempt); got < d || got >= 2*d {
				t.Fatalf("Delay(%d) = %s outside [%s, %s)", attempt, got, d, 2*d)
			}
		}
	}
}

func TestBackoffZeroBase(t *testing.T) {
	if got := (Backoff{}).Delay(3); got != 0 {
		t.Fatalf("zero backoff Delay = %s", got)
	}
}

func TestJitterBounds(t *testing.T) {
	d := time.Second
	for i := 0; i < 500; i++ {
		got := jitter(d)
		if got < 800*time.Millisecond || got >= 1200*time.Millisecond {
			t.Fatalf("jitter(%s) = %s outside ±20%%", d, got)
		}
	}
}
