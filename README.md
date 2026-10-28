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
- `ttl`: initial unavailable duration after a failed provider call.
- `max_ttl`: maximum unavailable duration after repeated failures. Must be higher than `ttl`.
- `providers`: map of provider name to optional health check and polling settings.

Provider fields (under `providers`):
- `url`: health endpoint to GET.
- `timeout`: request timeout in seconds.
- `per_model`: use per-model circuit keys.
- `ignore`: skip health checks for this provider.
- `poll`: background poll interval in seconds.

```json
{
  "ttl": 10,
  "max_ttl": 120,
  "providers": {
    "strata": {
      "url": "http://strata/health",
      "timeout": 2,
      "per_model": false,
      "ignore": false,
      "poll": 30
    }
  }
}
```

Values are seconds.


The plugin is compiled as a shared object and loaded by Bifrost at runtime.


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