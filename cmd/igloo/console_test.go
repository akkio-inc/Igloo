package main

import (
	"testing"
	"time"
)

func TestShortDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		time.Hour:                     "1h",
		90 * time.Minute:              "1h30m",
		5 * time.Second:               "5s",
		10 * time.Minute:              "10m",
		7 * 24 * time.Hour:            "7d",
		36 * time.Hour:                "36h",
		time.Hour + 30*time.Second:    "1h0m30s",
		1500 * time.Millisecond:       "1.5s",
		24*time.Hour + 30*time.Minute: "24h30m",
	} {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%s) = %q, want %q", d, got, want)
		}
	}
}
