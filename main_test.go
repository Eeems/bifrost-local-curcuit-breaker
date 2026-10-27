package main

import (
	"testing"
	"time"
)

func TestNextBackoffGrowsAndCaps(t *testing.T) {
	base := 10 * time.Second
	max := 80 * time.Second
	if got := nextBackoff(base, max, 1); got != 10*time.Second {
		t.Fatalf("expected 10s, got %s", got)
	}
	if got := nextBackoff(base, max, 2); got != 20*time.Second {
		t.Fatalf("expected 20s, got %s", got)
	}
	if got := nextBackoff(base, max, 10); got != 80*time.Second {
		t.Fatalf("expected cap 80s, got %s", got)
	}
	if got := nextBackoff(base, max, 100); got != 80*time.Second {
		t.Fatalf("expected cap 80s, got %s", got)
	}
}
