# Rate Limit Package

A comprehensive, idiomatic Go package providing multiple rate limiting algorithms for controlling request rates in your applications.

## Features

- 🚀 **Multiple Algorithms**: Token Bucket, Fixed Window, Sliding Window Log, and Leaky Bucket
- 🔧 **Simple API**: Clean, intuitive interface with the option pattern for configuration
- ⚡ **High Performance**: Optimized implementations with low overhead
- 🔒 **Thread-Safe**: All implementations are safe for concurrent use
- 📊 **Production-Ready**: Comprehensive tests and benchmarks included
- 📝 **Well-Documented**: Extensive examples and clear documentation

## Installation

```bash
go get github.com/alexjoedt/ratelimit
```

## Quick Start

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/alexjoedt/ratelimit"
)

func main() {
    // Create a rate limiter: 10 requests per second, burst of 20
    limiter := ratelimit.NewTokenBucket(
        ratelimit.WithRate(100*time.Millisecond), // 1/0.1s = 10/s
        ratelimit.WithCapacity(20),
    )
    defer limiter.Stop()
    
    // Check if request is allowed
    if _, ok := limiter.Take(); ok {
        fmt.Println("Request allowed")
    } else {
        fmt.Println("Rate limit exceeded")
    }
}
```

## Algorithms

### Token Bucket

**Best for**: APIs that can handle occasional bursts while maintaining average rate.

**How it works**: Maintains a bucket of tokens. Each request consumes a token. Tokens are refilled at a constant rate.

```go
limiter := ratelimit.NewTokenBucket(
    ratelimit.WithRate(time.Second),    // Add 1 token per second
    ratelimit.WithCapacity(10),         // Bucket holds up to 10 tokens
)
defer limiter.Stop()
```

**Pros**:
- Allows controlled bursts
- Simple to understand and implement
- Low memory overhead

**Cons**:
- Bursts can overwhelm downstream services
- Not suitable for strict rate limiting

### Fixed Window

**Best for**: Simple rate limiting with approximate guarantees and minimal memory usage.

**How it works**: Counts requests in fixed time windows. Counter resets at window boundaries.

```go
limiter := ratelimit.NewFixedWindow(
    ratelimit.WithLimit(100),           // Max 100 requests
    ratelimit.WithWindow(time.Second),  // Per second
)
defer limiter.Stop()
```

**Pros**:
- Very simple implementation
- Minimal memory usage
- Fast performance

**Cons**:
- Boundary issues (can allow 2x limit at window edges)
- Less accurate than sliding window

### Sliding Window Log

**Best for**: Precise rate limiting where accuracy is critical.

**How it works**: Tracks timestamps of individual requests. Slides the window continuously.

```go
limiter := ratelimit.NewSlidingWindowLog(
    ratelimit.WithLimit(100),           // Max 100 requests
    ratelimit.WithWindow(time.Second),  // Per second
)
defer limiter.Stop()
```

**Pros**:
- Most accurate rate limiting
- No boundary issues
- Precise guarantees

**Cons**:
- Higher memory usage (stores timestamps)
- Slightly slower than fixed window

### Leaky Bucket

**Best for**: Smoothing request bursts and maintaining steady output rate.

**How it works**: Requests go into a queue (bucket). Processed at a constant rate (leak).

```go
limiter := ratelimit.NewLeakyBucket(
    ratelimit.WithRate(100*time.Millisecond), // Process 1 every 100ms
    ratelimit.WithCapacity(20),               // Queue up to 20 requests
)
defer limiter.Stop()
```

**Pros**:
- Smooth, predictable output rate
- Protects downstream services from bursts
- Natural request queuing

**Cons**:
- Adds latency (requests wait in queue)
- Can reject requests when queue is full

## Usage Examples

### HTTP Middleware

```go
func rateLimitMiddleware(limiter ratelimit.Limiter) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            if _, ok := limiter.Take(); !ok {
                http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
                return
            }
            next.ServeHTTP(w, r)
        })
    }
}

// Usage
limiter := ratelimit.NewTokenBucket(
    ratelimit.WithRate(100*time.Millisecond),
    ratelimit.WithCapacity(20),
)
defer limiter.Stop()

mux := http.NewServeMux()
mux.HandleFunc("/api/data", dataHandler)

handler := rateLimitMiddleware(limiter)(mux)
http.ListenAndServe(":8080", handler)
```

### Per-User Rate Limiting

```go
type UserLimiter struct {
    limiters map[string]ratelimit.Limiter
    mu       sync.RWMutex
}

func NewUserLimiter() *UserLimiter {
    return &UserLimiter{
        limiters: make(map[string]ratelimit.Limiter),
    }
}

func (ul *UserLimiter) GetLimiter(userID string) ratelimit.Limiter {
    ul.mu.RLock()
    limiter, exists := ul.limiters[userID]
    ul.mu.RUnlock()
    
    if exists {
        return limiter
    }
    
    ul.mu.Lock()
    defer ul.mu.Unlock()
    
    // Double-check after acquiring write lock
    if limiter, exists := ul.limiters[userID]; exists {
        return limiter
    }
    
    // Create new limiter for user
    limiter = ratelimit.NewTokenBucket(
        ratelimit.WithRate(100*time.Millisecond),
        ratelimit.WithCapacity(20),
    )
    ul.limiters[userID] = limiter
    return limiter
}

func (ul *UserLimiter) Allow(userID string) bool {
    limiter := ul.GetLimiter(userID)
    _, ok := limiter.Take()
    return ok
}
```

### Blocking Wait for Availability

```go
// Wait for rate limit with timeout
limiter := ratelimit.NewTokenBucket(
    ratelimit.WithRate(time.Second),
    ratelimit.WithCapacity(1),
)
defer limiter.Stop()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if _, err := limiter.TakeWait(ctx); err != nil {
    log.Printf("Failed to acquire rate limit: %v", err)
    return
}

// Proceed with rate-limited operation
processRequest()
```

### Adaptive Rate Limiting

```go
type AdaptiveLimiter struct {
    limiter   ratelimit.Limiter
    mu        sync.Mutex
    errorRate float64
}

func (al *AdaptiveLimiter) RecordSuccess() {
    al.mu.Lock()
    defer al.mu.Unlock()
    
    al.errorRate *= 0.95 // Decay error rate
    
    // If error rate is low, consider increasing rate
    if al.errorRate < 0.01 {
        // Recreate limiter with higher rate
        al.limiter.Stop()
        al.limiter = ratelimit.NewTokenBucket(
            ratelimit.WithRate(50*time.Millisecond), // 20 req/s
            ratelimit.WithCapacity(40),
        )
    }
}

func (al *AdaptiveLimiter) RecordError() {
    al.mu.Lock()
    defer al.mu.Unlock()
    
    al.errorRate = al.errorRate*0.95 + 0.05
    
    // If error rate is high, decrease rate
    if al.errorRate > 0.1 {
        al.limiter.Stop()
        al.limiter = ratelimit.NewTokenBucket(
            ratelimit.WithRate(200*time.Millisecond), // 5 req/s
            ratelimit.WithCapacity(10),
        )
    }
}
```

### Circuit Breaker Integration

```go
type RateLimitedClient struct {
    limiter ratelimit.Limiter
    client  *http.Client
}

func (c *RateLimitedClient) Do(req *http.Request) (*http.Response, error) {
    // Wait for rate limit with timeout
    ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
    defer cancel()
    
    if _, err := c.limiter.TakeWait(ctx); err != nil {
        return nil, fmt.Errorf("rate limit timeout: %w", err)
    }
    
    return c.client.Do(req)
}
```

## API Reference

### Core Interface

```go
type Limiter interface {
    // Take attempts to acquire permission for a request
    Take() (time.Time, bool)
    
    // TakeWait blocks until permission is available
    TakeWait(ctx context.Context) (time.Time, error)
    
    // Stop releases all resources
    Stop()
}
```

### Configuration Options

```go
// Rate limiting interval (for Token Bucket, Leaky Bucket)
WithRate(rate time.Duration)

// Burst capacity (for Token Bucket, Leaky Bucket)
WithCapacity(capacity int)

// Request limit per window (for Fixed Window, Sliding Window)
WithLimit(limit int)

// Time window duration (for Fixed Window, Sliding Window)
WithWindow(window time.Duration)
```

### Constructor Functions

```go
// Algorithm-specific constructors
NewTokenBucket(opts ...Option) Limiter
NewFixedWindow(opts ...Option) Limiter
NewSlidingWindowLog(opts ...Option) Limiter
NewLeakyBucket(opts ...Option) Limiter
```

## Algorithm Comparison

| Algorithm | Memory | CPU | Accuracy | Burst Support | Use Case |
|-----------|--------|-----|----------|---------------|----------|
| Token Bucket | Low | Low | Good | Yes | General APIs with burst tolerance |
| Fixed Window | Very Low | Very Low | Moderate | No | Simple rate limiting, high throughput |
| Sliding Window | Medium | Medium | Excellent | No | Precise rate limiting, billing |
| Leaky Bucket | Low | Low | Good | Queues | Smooth output, protect downstream |

## Performance

Benchmarks on Apple M1:

```
BenchmarkTokenBucket-8              16254729      73.5 ns/op
BenchmarkFixedWindow-8              22891107      52.3 ns/op
BenchmarkSlidingWindowLog-8          4172064     287.0 ns/op
BenchmarkLeakyBucket-8              15638291      76.8 ns/op
BenchmarkConcurrentTokenBucket-8    48127416      24.9 ns/op
```

## Best Practices

1. **Always call Stop()**: Use `defer limiter.Stop()` to ensure proper cleanup
2. **Choose the right algorithm**: Match the algorithm to your use case
3. **Use context with TakeWait**: Always provide a timeout to prevent indefinite blocking
4. **Consider memory**: Sliding Window Log uses more memory than other algorithms
5. **Monitor and adjust**: Start conservative, monitor behavior, adjust rates as needed
6. **Per-resource limiting**: Use separate limiters for different resources/users
7. **Graceful degradation**: Handle rate limit errors gracefully with retries or backoff

## Common Patterns

### Graceful Degradation

```go
if _, ok := limiter.Take(); !ok {
    // Serve cached response or return partial data
    return serveFromCache()
}
// Proceed with normal processing
```

### Retry with Backoff

```go
for i := 0; i < 3; i++ {
    if _, ok := limiter.Take(); ok {
        return doRequest()
    }
    time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
}
return errors.New("rate limit exceeded after retries")
```

### Priority Queues

```go
// High-priority requests use one limiter
highPriorityLimiter := ratelimit.NewTokenBucket(
    ratelimit.WithRate(50*time.Millisecond),
    ratelimit.WithCapacity(100),
)

// Low-priority requests use a more restrictive limiter
lowPriorityLimiter := ratelimit.NewTokenBucket(
    ratelimit.WithRate(200*time.Millisecond),
    ratelimit.WithCapacity(10),
)
```

## Testing

Run tests:

```bash
go test ./ratelimit
```

Run benchmarks:

```bash
go test -bench=. ./ratelimit
```

Run with race detector:

```bash
go test -race ./ratelimit
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass
- Code is properly formatted (`go fmt`)
- New features include tests and documentation
- Benchmarks show no significant performance regression

## See Also

- [Go standard library rate limiting](https://pkg.go.dev/golang.org/x/time/rate)
- [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket)
- [Leaky Bucket Algorithm](https://en.wikipedia.org/wiki/Leaky_bucket)
