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

Plugin config JSON:

```json
{
  "ttl": 10,
  "max_ttl": 120
}
```

Values are seconds.

- `ttl`: initial unavailable duration after a failed provider call.
- `max_ttl`: maximum unavailable duration after repeated failures. Can be higher than `ttl`.

The plugin is compiled as a shared object and loaded by Bifrost at runtime.


## Build

```sh
GOTOOLCHAIN=go1.24.3 go mod tidy
GOTOOLCHAIN=go1.24.3 go test ./...
GOTOOLCHAIN=go1.24.3 CGO_ENABLED=1 go build -buildmode=plugin -o provider-circuit-breaker.so .
```