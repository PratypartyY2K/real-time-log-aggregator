package ingest

import (
	"sync"
	"time"
)

type MemoryRateLimiter struct {
	mu      sync.Mutex
	windows map[int64]*rateWindow
}

type rateWindow struct {
	windowStart time.Time
	count       int
}

func NewMemoryRateLimiter() *MemoryRateLimiter {
	return &MemoryRateLimiter{
		windows: make(map[int64]*rateWindow),
	}
}

func (l *MemoryRateLimiter) Allow(apiKeyID int64, limitPerSec int) bool {
	if apiKeyID <= 0 || limitPerSec <= 0 {
		return true
	}

	now := time.Now().UTC().Truncate(time.Second)

	l.mu.Lock()
	defer l.mu.Unlock()

	window := l.windows[apiKeyID]
	if window == nil || window.windowStart != now {
		l.windows[apiKeyID] = &rateWindow{
			windowStart: now,
			count:       1,
		}
		return true
	}

	if window.count >= limitPerSec {
		return false
	}

	window.count++
	return true
}
