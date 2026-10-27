# provider-circuit-breaker

Plugin for Bifrost AI Gateway.

It keeps a short in-memory cooldown for providers that fail. Other requests skip the provider while the cooldown is active, letting fallbacks run immediately.

## Behavior

- Treats timeouts, network errors, and 5xx responses as provider failures.
- Ignores 4xx client errors.
- Uses exponential backoff capped by `MaxTTL`.
- Resets a provider after a successful response.
- Cleans expired entries lazily.

## Config

```go
providercircuitbreaker.New(providercircuitbreaker.Config{
    TTL:    10 * time.Second,
    MaxTTL: 2 * time.Minute,
})
```

## Build

```sh
go mod tidy
go test ./...
```