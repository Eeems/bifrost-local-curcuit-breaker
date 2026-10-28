package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

type Config struct {
	TTL    time.Duration
	MaxTTL time.Duration
}

type providerState struct {
	until    time.Time
	failures int
}

var (
	pluginName               = "provider-circuit-breaker"
	baseTTL    time.Duration = 10 * time.Second
	maxTTL     time.Duration = 2 * time.Minute
	baseErr                  = &schemas.BifrostError{
		StatusCode: intPtr(503),
		Error: &schemas.ErrorField{
			Type:    strPtr("provider_unavailable"),
			Code:    strPtr("provider_circuit_open"),
			Message: "provider is temporarily unavailable",
		},
	}
	state sync.Map
)

var providerContextKey = schemas.BifrostContextKey("provider-circuit-breaker-key")

func Init(config any) error {
	cfg, ok := config.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid config format")
	}
	if ttl := durationFromConfig(cfg, "ttl"); ttl > 0 {
		baseTTL = ttl
	}
	if maxTTLVal := durationFromConfig(cfg, "max_ttl"); maxTTLVal > 0 {
		maxTTL = maxTTLVal
	}
	if maxTTL < baseTTL {
		maxTTL = baseTTL
	}
	return nil
}

func GetName() string {
	return pluginName
}

func Cleanup() error {
	return nil
}

func durationFromConfig(cfg map[string]any, key string) time.Duration {
	v, ok := cfg[key]
	if !ok {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return time.Duration(val) * time.Second
	case int:
		return time.Duration(val) * time.Second
	case time.Duration:
		return val
	case string:
		d, err := time.ParseDuration(val)
		if err == nil {
			return d
		}
	}
	return 0
}

func providerKeyFromRequest(req *schemas.BifrostRequest) string {
	if req == nil {
		return ""
	}
	var model string
	if req.ChatRequest != nil {
		model = req.ChatRequest.Model
	} else if req.ResponsesRequest != nil {
		model = req.ResponsesRequest.Model
	} else if req.TextCompletionRequest != nil {
		model = req.TextCompletionRequest.Model
	}
	// Use only provider portion so fallback models on same provider share cooldown.
	if i := strings.IndexByte(model, '/'); i >= 0 {
		return model[:i]
	}
	return model
}

func PreLLMHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	state.Range(func(key, value any) bool {
		entry, ok := value.(*providerState)
		if ok && time.Now().After(entry.until) {
			state.Delete(key)
		}
		return true
	})
	if baseTTL <= 0 || req == nil {
		return req, nil, nil
	}
	key := providerKeyFromRequest(req)
	if key == "" {
		return req, nil, nil
	}
	ctx.SetValue(providerContextKey, key)
	raw, ok := state.Load(key)
	if !ok {
		return req, nil, nil
	}
	entry := raw.(*providerState)
	now := time.Now()
	if now.After(entry.until) {
		state.Delete(key)
		return req, nil, nil
	}
	nowErr := *baseErr
	ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("provider-circuit-breaker: %s still unavailable until %s", key, entry.until.UTC().Format(time.RFC3339)))
	nowErr.Error.Message = fmt.Sprintf("%s is temporarily unavailable until %s", key, entry.until.UTC().Format(time.RFC3339))
	shortCircuit := &schemas.LLMPluginShortCircuit{
		Error: &nowErr,
	}
	ctx.SetValue("provider-circuit-breaker:skipped", true)
	return req, shortCircuit, nil
}

func nextBackoff(base, max time.Duration, failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures > 30 {
		failures = 30
	}
	delay := base * time.Duration(1<<uint(failures-1))
	if delay > max {
		delay = max
	}
	if delay < 0 {
		delay = max
	}
	return delay
}

func isLivenessFailure(err *schemas.BifrostError) bool {
	if err == nil {
		return false
	}
	if err.Error != nil && err.Error.Code != nil && *err.Error.Code == "provider_circuit_open" {
		return false
	}
	if err.StatusCode != nil && *err.StatusCode >= 500 && *err.StatusCode <= 599 {
		return true
	}
	errType := ""
	if err.Error != nil && err.Error.Type != nil {
		errType = *err.Error.Type
	}
	if errType == "timeout" || errType == "network_error" || errType == "connection_error" {
		return true
	}
	return false
}

func PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if bifrostErr == nil {
		rawKey, _ := ctx.Value(providerContextKey).(string)
		if rawKey != "" {
			state.Delete(rawKey)
		}
		return resp, nil, nil
	}
	if ctx.Value("provider-circuit-breaker:skipped") != nil {
		return resp, bifrostErr, nil
	}
	if !isLivenessFailure(bifrostErr) {
		return resp, bifrostErr, nil
	}
	provider := string(bifrostErr.ExtraFields.Provider)
	if provider == "" {
		return resp, bifrostErr, nil
	}
	failures := 1
	if raw, ok := state.Load(provider); ok {
		old := raw.(*providerState)
		failures = old.failures + 1
	}
	now := time.Now()
	state.Store(provider, &providerState{
		until:    now.Add(nextBackoff(baseTTL, maxTTL, failures)),
		failures: failures,
	})
	ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("provider-circuit-breaker: %s temporarily unavailable for %s due to failure %d", provider, nextBackoff(baseTTL, maxTTL, failures), failures))
	return resp, bifrostErr, nil
}

func intPtr(v int) *int {
	return &v
}

func strPtr(v string) *string {
	return &v
}
