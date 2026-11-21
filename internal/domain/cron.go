package domain

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ParseCron parses a standard 5-field expression or descriptor (@every 30s, @hourly, ...).
func ParseCron(expr string) (cron.Schedule, error) {
	s, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron_expr %q: %w", expr, err)
	}
	return s, nil
}

// NextCronRun returns the first activation strictly after `after`, evaluated in UTC.
func NextCronRun(s cron.Schedule, after time.Time) time.Time {
	return s.Next(after.UTC())
}
