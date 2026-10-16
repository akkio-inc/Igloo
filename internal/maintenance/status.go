package maintenance

import (
	"context"
	"time"
)

// LastPass reports when any replica last finished a pass over every table,
// as recorded in s. ok is false when none has been recorded.
func LastPass(ctx context.Context, s Schedule) (at time.Time, ok bool, err error) {
	runs, err := s.LastRuns(ctx, passJob)
	if err != nil {
		return time.Time{}, false, err
	}
	at, ok = runs[passJob]
	return at, ok, nil
}
