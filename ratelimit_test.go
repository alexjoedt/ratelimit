package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTokenBucket_Basic tests basic token bucket functionality.
func TestTokenBucket_Basic(t *testing.T) {
	limiter := NewTokenBucket(
		WithRate(100*time.Millisecond),
		WithCapacity(3),
	)
	defer limiter.Stop()

	// Should allow burst of 3
	for i := 0; i < 3; i++ {
		if _, ok := limiter.Take(); !ok {
			t.Errorf("Request %d should be allowed (burst)", i+1)
		}
	}

	// 4th request should be denied
	if _, ok := limiter.Take(); ok {
		t.Error("4th request should be denied")
	}

	// Wait for token refill
	time.Sleep(150 * time.Millisecond)

	// Should allow 1 more request
	if _, ok := limiter.Take(); !ok {
		t.Error("Request after refill should be allowed")
	}
}

// TestTokenBucket_Refill tests that tokens are refilled correctly.
func TestTokenBucket_Refill(t *testing.T) {
	limiter := NewTokenBucket(
		WithRate(50*time.Millisecond),
		WithCapacity(1),
	)
	defer limiter.Stop()

	// Use the initial token
	if _, ok := limiter.Take(); !ok {
		t.Fatal("Initial token should be available")
	}

	// Should be denied immediately
	if _, ok := limiter.Take(); ok {
		t.Error("Second request should be denied")
	}

	// Wait for refill
	time.Sleep(70 * time.Millisecond)

	// Should be allowed now
	if _, ok := limiter.Take(); !ok {
		t.Error("Request after refill should be allowed")
	}
}

// TestTokenBucket_TakeWait tests blocking wait functionality.
func TestTokenBucket_TakeWait(t *testing.T) {
	limiter := NewTokenBucket(
		WithRate(100*time.Millisecond),
		WithCapacity(1),
	)
	defer limiter.Stop()

	// Use first token
	limiter.Take()

	// Wait for next token
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := limiter.TakeWait(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("TakeWait failed: %v", err)
	}

	// Should have waited approximately 100ms
	if elapsed < 80*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("Expected ~100ms wait, got %v", elapsed)
	}
}

// TestTokenBucket_TakeWaitTimeout tests timeout behavior.
func TestTokenBucket_TakeWaitTimeout(t *testing.T) {
	limiter := NewTokenBucket(
		WithRate(time.Second),
		WithCapacity(1),
	)
	defer limiter.Stop()

	// Use first token
	limiter.Take()

	// Wait with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := limiter.TakeWait(ctx)
	if err == nil {
		t.Error("Expected timeout error")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", err)
	}
}

// TestFixedWindow_Basic tests basic fixed window functionality.
func TestFixedWindow_Basic(t *testing.T) {
	limiter := NewFixedWindow(
		WithLimit(3),
		WithWindow(100*time.Millisecond),
	)
	defer limiter.Stop()

	// Should allow 3 requests
	for i := 0; i < 3; i++ {
		if _, ok := limiter.Take(); !ok {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 4th request should be denied
	if _, ok := limiter.Take(); ok {
		t.Error("4th request should be denied")
	}

	// Wait for window reset
	time.Sleep(150 * time.Millisecond)

	// Should allow requests again
	if _, ok := limiter.Take(); !ok {
		t.Error("Request after window reset should be allowed")
	}
}

// TestFixedWindow_WindowReset tests that windows reset correctly.
func TestFixedWindow_WindowReset(t *testing.T) {
	limiter := NewFixedWindow(
		WithLimit(2),
		WithWindow(100*time.Millisecond),
	)
	defer limiter.Stop()

	// Use quota
	limiter.Take()
	limiter.Take()

	// Should be denied
	if _, ok := limiter.Take(); ok {
		t.Error("Request should be denied in current window")
	}

	// Wait for new window
	time.Sleep(120 * time.Millisecond)

	// Should have full quota again
	allowed := 0
	for i := 0; i < 3; i++ {
		if _, ok := limiter.Take(); ok {
			allowed++
		}
	}

	if allowed != 2 {
		t.Errorf("Expected 2 requests allowed in new window, got %d", allowed)
	}
}

// TestSlidingWindowLog_Basic tests basic sliding window log functionality.
func TestSlidingWindowLog_Basic(t *testing.T) {
	limiter := NewSlidingWindowLog(
		WithLimit(3),
		WithWindow(200*time.Millisecond),
	)
	defer limiter.Stop()

	// Should allow 3 requests
	for i := 0; i < 3; i++ {
		if _, ok := limiter.Take(); !ok {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 4th request should be denied
	if _, ok := limiter.Take(); ok {
		t.Error("4th request should be denied")
	}
}

// TestSlidingWindowLog_Sliding tests that old requests expire correctly.
func TestSlidingWindowLog_Sliding(t *testing.T) {
	limiter := NewSlidingWindowLog(
		WithLimit(2),
		WithWindow(100*time.Millisecond),
	)
	defer limiter.Stop()

	// Make 2 requests
	time1, _ := limiter.Take()
	limiter.Take()

	// Should be denied
	if _, ok := limiter.Take(); ok {
		t.Error("3rd request should be denied")
	}

	// Wait for first request to expire (wait slightly longer to account for timing)
	waitTime := time.Until(time1.Add(110 * time.Millisecond))
	if waitTime > 0 {
		time.Sleep(waitTime)
	}

	// Should be allowed now (first request expired)
	if _, ok := limiter.Take(); !ok {
		t.Error("Request after window slide should be allowed")
	}
}

// TestSlidingWindowLog_Precision tests precision of sliding window.
func TestSlidingWindowLog_Precision(t *testing.T) {
	limiter := NewSlidingWindowLog(
		WithLimit(2),
		WithWindow(200*time.Millisecond),
	)
	defer limiter.Stop()

	// Request at t=0
	limiter.Take()

	// Wait 100ms
	time.Sleep(100 * time.Millisecond)

	// Request at t=100
	limiter.Take()

	// Should be denied (2 requests in 200ms window)
	if _, ok := limiter.Take(); ok {
		t.Error("Request should be denied")
	}

	// Wait another 110ms (total 210ms from first request)
	time.Sleep(110 * time.Millisecond)

	// First request should have expired, should be allowed
	if _, ok := limiter.Take(); !ok {
		t.Error("Request should be allowed after first request expired")
	}
}

// TestLeakyBucket_Basic tests basic leaky bucket functionality.
func TestLeakyBucket_Basic(t *testing.T) {
	limiter := NewLeakyBucket(
		WithRate(100*time.Millisecond),
		WithCapacity(3),
	)
	defer limiter.Stop()

	// Should accept up to capacity
	for i := 0; i < 3; i++ {
		if _, ok := limiter.Take(); !ok {
			t.Errorf("Request %d should be queued", i+1)
		}
	}

	// 4th request should be rejected (queue full)
	if _, ok := limiter.Take(); ok {
		t.Error("Request should be rejected when queue is full")
	}
}

// TestLeakyBucket_Processing tests that requests are processed at correct rate.
func TestLeakyBucket_Processing(t *testing.T) {
	limiter := NewLeakyBucket(
		WithRate(50*time.Millisecond),
		WithCapacity(5),
	)
	defer limiter.Stop()

	// Fill queue
	for i := 0; i < 5; i++ {
		limiter.Take()
	}

	// Queue should be full
	if _, ok := limiter.Take(); ok {
		t.Error("Queue should be full")
	}

	// Wait for some processing
	time.Sleep(180 * time.Millisecond) // Should process ~3 requests

	// Should be able to add requests again
	allowed := 0
	for i := 0; i < 5; i++ {
		if _, ok := limiter.Take(); ok {
			allowed++
		}
	}

	if allowed < 2 || allowed > 4 {
		t.Errorf("Expected 2-4 slots available, got %d", allowed)
	}
}

// TestConcurrentAccess tests thread safety of all limiters.
func TestConcurrentAccess(t *testing.T) {
	limiters := []struct {
		name    string
		limiter Limiter
	}{
		{"TokenBucket", NewTokenBucket(WithRate(time.Millisecond), WithCapacity(100))},
		{"FixedWindow", NewFixedWindow(WithLimit(100), WithWindow(100*time.Millisecond))},
		{"SlidingWindow", NewSlidingWindowLog(WithLimit(100), WithWindow(100*time.Millisecond))},
		{"LeakyBucket", NewLeakyBucket(WithRate(time.Millisecond), WithCapacity(100))},
	}

	for _, tc := range limiters {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.limiter.Stop()

			var wg sync.WaitGroup
			var allowed atomic.Int32
			goroutines := 50
			requestsPerGoroutine := 10

			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < requestsPerGoroutine; j++ {
						if _, ok := tc.limiter.Take(); ok {
							allowed.Add(1)
						}
					}
				}()
			}

			wg.Wait()

			// Should have allowed some requests without panicking
			if allowed.Load() == 0 {
				t.Error("No requests were allowed")
			}
		})
	}
}

// TestStopCleansUp tests that Stop properly cleans up resources.
func TestStopCleansUp(t *testing.T) {
	limiters := []Limiter{
		NewTokenBucket(WithRate(time.Millisecond), WithCapacity(1)),
		NewFixedWindow(WithLimit(1), WithWindow(time.Millisecond)),
		NewSlidingWindowLog(WithLimit(1), WithWindow(time.Millisecond)),
		NewLeakyBucket(WithRate(time.Millisecond), WithCapacity(1)),
	}

	for _, limiter := range limiters {
		// Should not panic
		limiter.Stop()
		limiter.Stop() // Second stop should be safe
	}
}

// TestConfigDefaults tests that default configuration works.
func TestConfigDefaults(t *testing.T) {
	limiter := NewTokenBucket() // Use defaults
	defer limiter.Stop()

	// Should work with defaults
	if _, ok := limiter.Take(); !ok {
		t.Error("Default configuration should allow at least one request")
	}
}

// TestMultipleAlgorithms tests creating limiters with different algorithms.
func TestMultipleAlgorithms(t *testing.T) {
	tests := []struct {
		name    string
		limiter Limiter
	}{
		{
			name:    "TokenBucket",
			limiter: NewTokenBucket(WithRate(time.Millisecond), WithCapacity(5)),
		},
		{
			name:    "FixedWindow",
			limiter: NewFixedWindow(WithLimit(5), WithWindow(10*time.Millisecond)),
		},
		{
			name:    "SlidingWindow",
			limiter: NewSlidingWindowLog(WithLimit(5), WithWindow(10*time.Millisecond)),
		},
		{
			name:    "LeakyBucket",
			limiter: NewLeakyBucket(WithRate(time.Millisecond), WithCapacity(5)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.limiter.Stop()

			// Should allow at least one request
			if _, ok := tt.limiter.Take(); !ok {
				t.Errorf("%s should allow at least one request", tt.name)
			}
		})
	}
}

// BenchmarkTokenBucket benchmarks token bucket performance.
func BenchmarkTokenBucket(b *testing.B) {
	limiter := NewTokenBucket(
		WithRate(time.Nanosecond),
		WithCapacity(1000000),
	)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Take()
	}
}

// BenchmarkFixedWindow benchmarks fixed window performance.
func BenchmarkFixedWindow(b *testing.B) {
	limiter := NewFixedWindow(
		WithLimit(1000000),
		WithWindow(time.Hour),
	)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Take()
	}
}

// BenchmarkSlidingWindowLog benchmarks sliding window log performance.
func BenchmarkSlidingWindowLog(b *testing.B) {
	limiter := NewSlidingWindowLog(
		WithLimit(1000),
		WithWindow(time.Second),
	)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Take()
	}
}

// BenchmarkLeakyBucket benchmarks leaky bucket performance.
func BenchmarkLeakyBucket(b *testing.B) {
	limiter := NewLeakyBucket(
		WithRate(time.Nanosecond),
		WithCapacity(1000000),
	)
	defer limiter.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Take()
	}
}

// BenchmarkConcurrentTokenBucket benchmarks concurrent access to token bucket.
func BenchmarkConcurrentTokenBucket(b *testing.B) {
	limiter := NewTokenBucket(
		WithRate(time.Nanosecond),
		WithCapacity(1000000),
	)
	defer limiter.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Take()
		}
	})
}
