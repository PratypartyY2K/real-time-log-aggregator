package ingest

import "testing"

func TestMemoryRateLimiterEnforcesPerKeyLimit(t *testing.T) {
	limiter := NewMemoryRateLimiter()

	if !limiter.Allow(1, 2) {
		t.Fatal("expected first request to pass")
	}
	if !limiter.Allow(1, 2) {
		t.Fatal("expected second request to pass")
	}
	if limiter.Allow(1, 2) {
		t.Fatal("expected third request in same window to be rate limited")
	}
	if !limiter.Allow(2, 2) {
		t.Fatal("expected separate api key to have independent budget")
	}
}

func TestMemoryRateLimiterSkipsNonPositiveLimits(t *testing.T) {
	limiter := NewMemoryRateLimiter()

	if !limiter.Allow(1, 0) {
		t.Fatal("expected zero limit to skip limiter")
	}
	if !limiter.Allow(0, 10) {
		t.Fatal("expected zero api key id to skip limiter")
	}
}
