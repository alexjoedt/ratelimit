// Package ratelimit provides multiple rate limiting algorithms for controlling
// request rates in Go applications.
//
// The package offers several rate limiting strategies:
//   - Token Bucket: Allows bursts while maintaining average rate
//   - Fixed Window: Simple counter-based with fixed time windows
//   - Sliding Window Log: Precise rate limiting using request timestamps
//   - Leaky Bucket: Smooth rate limiting with queue-based processing
//
// # Basic Usage
//
//	// Token Bucket (allows bursts)
//	limiter := ratelimit.NewTokenBucket(
//	    ratelimit.WithRate(time.Second),
//	    ratelimit.WithCapacity(10),
//	)
//	defer limiter.Stop()
//
//	if _, ok := limiter.Take(); ok {
//	    // Request allowed
//	} else {
//	    // Rate limit exceeded
//	}
//
// # Algorithm Selection
//
// Use Token Bucket when:
//   - You want to allow bursts of traffic
//   - Average rate control is sufficient
//   - Low overhead is important
//
// Use Fixed Window when:
//   - Simplest implementation needed
//   - Approximate rate limiting is acceptable
//   - Memory efficiency is critical
//
// Use Sliding Window Log when:
//   - Precise rate limiting is required
//   - Avoiding boundary issues is important
//   - You can afford slightly higher memory usage
//
// Use Leaky Bucket when:
//   - Smooth, consistent output rate is needed
//   - You want to queue excess requests
//   - Preventing bursts is critical
package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter is the core interface for all rate limiting implementations.
// It provides methods to check if a request is allowed and clean up resources.
type Limiter interface {
	// Take attempts to acquire permission for a request.
	// Returns the current time and true if the request is allowed,
	// or zero time and false if the rate limit is exceeded.
	Take() (time.Time, bool)

	// TakeWait blocks until permission for a request is available.
	// Returns the time when permission was granted.
	// Use context to set timeout or cancel the wait.
	TakeWait(ctx context.Context) (time.Time, error)

	// Stop stops the rate limiter and releases all resources.
	// After calling Stop, the limiter should not be used.
	Stop()
}

// Config holds the configuration for a rate limiter.
type Config struct {
	Rate     time.Duration // Time between allowed requests
	Capacity int           // Maximum burst capacity (bucket size)
	Limit    int           // Maximum requests per window (for window-based algorithms)
	Window   time.Duration // Time window duration (for window-based algorithms)
}

// Option is a function that configures a Config.
type Option func(*Config)

// WithRate sets the rate limiting interval (e.g., time.Second for 1 req/sec).
// Used by TokenBucket and LeakyBucket algorithms.
func WithRate(rate time.Duration) Option {
	return func(c *Config) {
		c.Rate = rate
	}
}

// WithCapacity sets the burst capacity (number of tokens in bucket).
// Used by TokenBucket algorithm.
func WithCapacity(capacity int) Option {
	return func(c *Config) {
		c.Capacity = capacity
	}
}

// WithLimit sets the maximum number of requests allowed per window.
// Used by FixedWindow and SlidingWindowLog algorithms.
func WithLimit(limit int) Option {
	return func(c *Config) {
		c.Limit = limit
	}
}

// WithWindow sets the time window duration.
// Used by FixedWindow and SlidingWindowLog algorithms.
func WithWindow(window time.Duration) Option {
	return func(c *Config) {
		c.Window = window
	}
}

// NewTokenBucket creates a new token bucket rate limiter.
//
// Token bucket allows bursts up to the capacity while maintaining an average rate.
//
// Example:
//
//	// Allow 100 requests per second with burst capacity of 50
//	limiter := ratelimit.NewTokenBucket(
//	    ratelimit.WithRate(10*time.Millisecond), // 1/0.01s = 100/s
//	    ratelimit.WithCapacity(50),
//	)
func NewTokenBucket(opts ...Option) Limiter {
	cfg := &Config{
		Rate:     time.Second,
		Capacity: 1,
		Limit:    1,
		Window:   time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return newTokenBucket(cfg)
}

// NewFixedWindow creates a new fixed window rate limiter.
//
// Fixed window uses a simple counter that resets at fixed intervals.
//
// Example:
//
//	// Allow 100 requests per second
//	limiter := ratelimit.NewFixedWindow(
//	    ratelimit.WithLimit(100),
//	    ratelimit.WithWindow(time.Second),
//	)
func NewFixedWindow(opts ...Option) Limiter {
	cfg := &Config{
		Rate:     time.Second,
		Capacity: 1,
		Limit:    1,
		Window:   time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return newFixedWindow(cfg)
}

// NewSlidingWindowLog creates a new sliding window log rate limiter.
//
// Sliding window log tracks individual request timestamps for precise rate limiting.
//
// Example:
//
//	// Allow exactly 100 requests per second
//	limiter := ratelimit.NewSlidingWindowLog(
//	    ratelimit.WithLimit(100),
//	    ratelimit.WithWindow(time.Second),
//	)
func NewSlidingWindowLog(opts ...Option) Limiter {
	cfg := &Config{
		Rate:     time.Second,
		Capacity: 1,
		Limit:    1,
		Window:   time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return newSlidingWindowLog(cfg)
}

// NewLeakyBucket creates a new leaky bucket rate limiter.
//
// Leaky bucket processes requests at a constant rate, smoothing out bursts.
//
// Example:
//
//	// Process requests at 10 per second with queue capacity of 20
//	limiter := ratelimit.NewLeakyBucket(
//	    ratelimit.WithRate(100*time.Millisecond), // 1/0.1s = 10/s
//	    ratelimit.WithCapacity(20),
//	)
func NewLeakyBucket(opts ...Option) Limiter {
	cfg := &Config{
		Rate:     time.Second,
		Capacity: 1,
		Limit:    1,
		Window:   time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return newLeakyBucket(cfg)
}

// =============================================================================
// Token Bucket Implementation
// =============================================================================

type token struct{}

type tokenBucket struct {
	bucket chan token
	rate   time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

func newTokenBucket(cfg *Config) Limiter {
	tb := &tokenBucket{
		bucket: make(chan token, cfg.Capacity),
		rate:   cfg.Rate,
	}

	// Pre-fill bucket with tokens
	for i := 0; i < cfg.Capacity; i++ {
		tb.bucket <- token{}
	}

	// Start token refill goroutine
	ctx, cancel := context.WithCancel(context.Background())
	tb.ctx = ctx
	tb.cancel = cancel
	go tb.refill()

	return tb
}

func (tb *tokenBucket) Take() (time.Time, bool) {
	select {
	case <-tb.bucket:
		return time.Now(), true
	default:
		return time.Time{}, false
	}
}

func (tb *tokenBucket) TakeWait(ctx context.Context) (time.Time, error) {
	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-tb.bucket:
		return time.Now(), nil
	}
}

func (tb *tokenBucket) Stop() {
	if tb.cancel != nil {
		tb.cancel()
	}
}

func (tb *tokenBucket) refill() {
	ticker := time.NewTicker(tb.rate)
	defer ticker.Stop()

	for {
		select {
		case <-tb.ctx.Done():
			return
		case <-ticker.C:
			select {
			case tb.bucket <- token{}:
				// Token added successfully
			default:
				// Bucket is full, skip
			}
		}
	}
}

// =============================================================================
// Fixed Window Implementation
// =============================================================================

type fixedWindow struct {
	mu          sync.Mutex
	limit       int
	window      time.Duration
	count       int
	windowStart time.Time
	ctx         context.Context
	cancel      context.CancelFunc
	waitCh      chan struct{}
	stopOnce    sync.Once
}

func newFixedWindow(cfg *Config) Limiter {
	ctx, cancel := context.WithCancel(context.Background())
	fw := &fixedWindow{
		limit:       cfg.Limit,
		window:      cfg.Window,
		windowStart: time.Now(),
		ctx:         ctx,
		cancel:      cancel,
		waitCh:      make(chan struct{}, 1),
	}
	go fw.resetWorker()
	return fw
}

func (fw *fixedWindow) Take() (time.Time, bool) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	now := time.Now()

	// Check if we need to reset the window
	if now.Sub(fw.windowStart) >= fw.window {
		fw.count = 0
		fw.windowStart = now
	}

	if fw.count < fw.limit {
		fw.count++
		return now, true
	}

	return time.Time{}, false
}

func (fw *fixedWindow) TakeWait(ctx context.Context) (time.Time, error) {
	for {
		if now, ok := fw.Take(); ok {
			return now, nil
		}

		// Wait for next window
		fw.mu.Lock()
		nextWindow := fw.windowStart.Add(fw.window)
		fw.mu.Unlock()

		waitDuration := time.Until(nextWindow)
		if waitDuration > 0 {
			timer := time.NewTimer(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return time.Time{}, ctx.Err()
			case <-fw.ctx.Done():
				timer.Stop()
				return time.Time{}, context.Canceled
			case <-timer.C:
				// Window reset, try again
			}
		}
	}
}

func (fw *fixedWindow) Stop() {
	fw.stopOnce.Do(func() {
		if fw.cancel != nil {
			fw.cancel()
		}
	})
}

func (fw *fixedWindow) resetWorker() {
	ticker := time.NewTicker(fw.window)
	defer ticker.Stop()

	for {
		select {
		case <-fw.ctx.Done():
			return
		case <-ticker.C:
			fw.mu.Lock()
			now := time.Now()
			if now.Sub(fw.windowStart) >= fw.window {
				fw.count = 0
				fw.windowStart = now
				// Notify waiting goroutines
				select {
				case fw.waitCh <- struct{}{}:
				default:
				}
			}
			fw.mu.Unlock()
		}
	}
}

// =============================================================================
// Sliding Window Log Implementation
// =============================================================================

type slidingWindowLog struct {
	mu       sync.Mutex
	limit    int
	window   time.Duration
	requests []time.Time
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func newSlidingWindowLog(cfg *Config) Limiter {
	ctx, cancel := context.WithCancel(context.Background())
	swl := &slidingWindowLog{
		limit:    cfg.Limit,
		window:   cfg.Window,
		requests: make([]time.Time, 0, cfg.Limit),
		ctx:      ctx,
		cancel:   cancel,
	}
	go swl.cleanupWorker()
	return swl
}

func (swl *slidingWindowLog) Take() (time.Time, bool) {
	swl.mu.Lock()
	defer swl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-swl.window)

	// Remove expired requests - keep only those after cutoff
	validRequests := swl.requests[:0]
	for _, t := range swl.requests {
		if t.After(cutoff) {
			validRequests = append(validRequests, t)
		}
	}
	swl.requests = validRequests

	// Check if we can accept the request
	if len(swl.requests) < swl.limit {
		swl.requests = append(swl.requests, now)
		return now, true
	}

	return time.Time{}, false
}

func (swl *slidingWindowLog) TakeWait(ctx context.Context) (time.Time, error) {
	for {
		swl.mu.Lock()
		now := time.Now()
		cutoff := now.Add(-swl.window)

		// Remove expired requests - keep only those after cutoff
		validRequests := swl.requests[:0]
		for _, t := range swl.requests {
			if t.After(cutoff) {
				validRequests = append(validRequests, t)
			}
		}
		swl.requests = validRequests

		if len(swl.requests) < swl.limit {
			swl.requests = append(swl.requests, now)
			swl.mu.Unlock()
			return now, nil
		}

		// Calculate wait time until oldest request expires
		var waitDuration time.Duration
		if len(swl.requests) > 0 {
			oldestValid := swl.requests[0]
			waitDuration = time.Until(oldestValid.Add(swl.window))
		}
		swl.mu.Unlock()

		if waitDuration > 0 {
			timer := time.NewTimer(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return time.Time{}, ctx.Err()
			case <-swl.ctx.Done():
				timer.Stop()
				return time.Time{}, context.Canceled
			case <-timer.C:
				// Try again
			}
		}
	}
}

func (swl *slidingWindowLog) Stop() {
	swl.stopOnce.Do(func() {
		if swl.cancel != nil {
			swl.cancel()
		}
	})
}

func (swl *slidingWindowLog) cleanupWorker() {
	ticker := time.NewTicker(swl.window / 2)
	defer ticker.Stop()

	for {
		select {
		case <-swl.ctx.Done():
			return
		case <-ticker.C:
			swl.mu.Lock()
			now := time.Now()
			cutoff := now.Add(-swl.window)

			// Keep only valid requests
			validRequests := swl.requests[:0]
			for _, t := range swl.requests {
				if t.After(cutoff) {
					validRequests = append(validRequests, t)
				}
			}
			swl.requests = validRequests
			swl.mu.Unlock()
		}
	}
}

// =============================================================================
// Leaky Bucket Implementation
// =============================================================================

type leakyBucket struct {
	queue    chan struct{}
	rate     time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func newLeakyBucket(cfg *Config) Limiter {
	ctx, cancel := context.WithCancel(context.Background())
	lb := &leakyBucket{
		queue:  make(chan struct{}, cfg.Capacity),
		rate:   cfg.Rate,
		ctx:    ctx,
		cancel: cancel,
	}
	go lb.leak()
	return lb
}

func (lb *leakyBucket) Take() (time.Time, bool) {
	select {
	case lb.queue <- struct{}{}:
		return time.Now(), true
	default:
		return time.Time{}, false
	}
}

func (lb *leakyBucket) TakeWait(ctx context.Context) (time.Time, error) {
	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-lb.ctx.Done():
		return time.Time{}, context.Canceled
	case lb.queue <- struct{}{}:
		return time.Now(), nil
	}
}

func (lb *leakyBucket) Stop() {
	lb.stopOnce.Do(func() {
		if lb.cancel != nil {
			lb.cancel()
		}
	})
}

func (lb *leakyBucket) leak() {
	ticker := time.NewTicker(lb.rate)
	defer ticker.Stop()

	for {
		select {
		case <-lb.ctx.Done():
			return
		case <-ticker.C:
			select {
			case <-lb.queue:
				// Request processed
			default:
				// Queue is empty
			}
		}
	}
}
