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

Top-level:
- `ttl`: how long a failed provider is avoided after an error (seconds or [Go time.ParseDuration](https://pkg.go.dev/time#ParseDuration)).
- `max_ttl`: longest time a failed provider is avoided after repeated errors (seconds or [Go time.ParseDuration](https://pkg.go.dev/time#ParseDuration)).
- `per_model`: by default, each model has its own circuit; set false to share one circuit per provider.
- `providers`: optional health check and polling settings per provider.

Provider fields (under `providers`):
- `url`: health endpoint URL to query when checking if a provider is up.
- `timeout`: health request timeout (seconds or [Go time.ParseDuration](https://pkg.go.dev/time#ParseDuration)).
- `per_model`: override the global per-model setting for this provider.
- `enabled`: if this provider configuration is enabled, defaults to true.
- `skip_check`: skip health checks but still use circuit breaker on failures.
- `poll`: how often to check provider health in the background (seconds or [Go time.ParseDuration](https://pkg.go.dev/time#ParseDuration)).

```json
{
  "ttl": 10,
  "max_ttl": 120,
  "providers": {
    "strata": {
      "url": "http://localhost:8080/health",
      "timeout": 2,
      "per_model": false,
      "ignore": false,
      "poll": "30s"
    }
  }
}
```

## Build

```sh
go mod tidy
go test ./...
CGO_ENABLED=1 \
GOOS=linux \
GOARCH=amd64 \
GOTOOLCHAIN=go1.27.3 \
go build -a -trimpath -buildmode=plugin -o provider-circuit-breaker.so .
```
