package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
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

type providerHealthConfig struct {
	URL       string
	PerModel  bool
	Timeout   time.Duration
	Enabled   bool
	SkipCheck bool
	Poll      time.Duration
}

var (
	pluginName                  = "provider-circuit-breaker"
	baseTTL       time.Duration = 10 * time.Second
	maxTTL        time.Duration = 2 * time.Minute
	perModel      bool          = true
	ignoreMissing bool          = false
	baseErr                     = &schemas.BifrostError{
		Error: &schemas.ErrorField{
			Type:    strPtr("provider_unavailable"),
			Code:    strPtr("provider_circuit_open"),
			Message: "provider is temporarily unavailable",
		},
	}
	state                    sync.Map
	providerConfigs          map[string]providerHealthConfig
	statusServiceUnavailable int = http.StatusServiceUnavailable
	providerContextKey           = schemas.BifrostContextKey("provider-circuit-breaker-key")
	cancelPolls              context.CancelFunc
)

func Init(config any) error {
	cfg, ok := config.(map[string]any)
	if !ok {
		return fmt.Errorf("config must be object")
	}
	for k := range cfg {
		switch k {
		case "ttl", "max_ttl", "per_model", "ignore_missing", "providers":
		default:
			return fmt.Errorf("unexpected config key %s", k)
		}
	}
	if ttl := durationFromConfig(cfg, "ttl"); ttl > 0 {
		baseTTL = ttl
	} else {
		return fmt.Errorf("baseTTL must be > 0")
	}
	if maxTTLVal := durationFromConfig(cfg, "max_ttl"); maxTTLVal > 0 {
		maxTTL = maxTTLVal
	} else {
		return fmt.Errorf("maxTTL must be > 0")
	}
	if maxTTL < baseTTL {
		return fmt.Errorf("maxTTL must be >= baseTTL")
	}
	if v, ok := cfg["ignore_missing"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("ignore_missing must be bool")
		}
		ignoreMissing = b
	}
	if v, ok := cfg["per_model"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("per_model must be bool")
		}
		perModel = b
	}
	if raw, ok := cfg["providers"]; ok {
		providersRaw, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("providers must be an object")
		}
		providerConfigs = map[string]providerHealthConfig{}
		for k, v := range providersRaw {
			cfg := providerHealthConfig{Enabled: true}
			switch val := v.(type) {
			case map[string]any:
				for field := range val {
					switch field {
					case "url", "timeout", "per_model", "enabled", "skip_check", "poll":
					default:
						return fmt.Errorf("provider %s unexpected key %s", k, field)
					}
				}
				if v, ok := val["url"]; ok {
					u, ok := v.(string)
					if !ok || u == "" {
						return fmt.Errorf("provider %s url must be a non-empty string", k)
					}
					cfg.URL = u
				}
				if v, ok := val["per_model"]; ok {
					b, ok := v.(bool)
					if !ok {
						return fmt.Errorf("provider %s per_model must be bool", k)
					}
					cfg.PerModel = b
				}
				if _, ok := val["timeout"]; ok {
					d := durationFromConfig(val, "timeout")
					if d <= 0 {
						return fmt.Errorf("provider %s timeout must be > 0", k)
					}
					cfg.Timeout = d
				}
				if v, ok := val["enabled"]; ok {
					b, ok := v.(bool)
					if !ok {
						return fmt.Errorf("provider %s enabled must be bool", k)
					}
					cfg.Enabled = b
				}
				if v, ok := val["skip_check"]; ok {
					b, ok := v.(bool)
					if !ok {
						return fmt.Errorf("provider %s skip_check must be bool", k)
					}
					cfg.SkipCheck = b
				}
				if _, ok := val["poll"]; ok {
					d := durationFromConfig(val, "poll")
					if d <= 0 {
						return fmt.Errorf("provider %s poll must be > 0", k)
					}
					cfg.Poll = d
				}
				if cfg.SkipCheck && cfg.Poll > 0 {
					return fmt.Errorf("provider %s cannot set poll with skip_check", k)
				}
			default:
				return fmt.Errorf("provider %s config must be object", k)
			}
			providerConfigs[k] = cfg
		}
	}
	hasPoll := false
	ctx, cancel := context.WithCancel(context.Background())
	cancelPolls = cancel
	for name, pc := range providerConfigs {
		if pc.Poll <= 0 || !pc.Enabled || pc.URL == "" {
			continue
		}
		hasPoll = true
		go func(name string, pc providerHealthConfig) {
			ticker := time.NewTicker(pc.Poll)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if raw, ok := state.Load(name); ok {
						entry := raw.(*providerState)
						if time.Now().Before(entry.until) {
							continue
						}
					}
					if !providerHealthy(name) {
						markProviderFailed(name)
					}
				}
			}
		}(name, pc)
	}
	if !hasPoll {
		cancelPolls = nil
		cancel()
		return nil
	}
	return nil
}

func Cleanup() error {
	if cancelPolls != nil {
		cancelPolls()
	}
	return nil
}

func GetName() string {
	return pluginName
}

func PreLLMHook(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	state.Range(func(key, value any) bool {
		entry, ok := value.(*providerState)
		if ok && time.Now().After(entry.until) {
			state.Delete(key)
		}
		return true
	})
	if req == nil {
		return req, nil, nil
	}
	key := providerKeyFromRequest(req)
	if key == "" {
		return req, nil, nil
	}
	if ignoreMissing && !isConfigured(providerKey(key)) {
		ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("%s prehook skipped", key))
		return req, nil, nil
	}
	if ctx.Value(providerContextKey) == key {
		return req, nil, nil
	}
	if modelFailed(key) {
		// pass
	} else if !providerHealthy(key) {
		markProviderFailed(providerKey(key))
	} else {
		ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("%s is healthy", key))
		ctx.SetValue(providerContextKey, key)
		raw, ok := state.Load(key)
		if !ok {
			return req, nil, nil
		}
		entry := raw.(*providerState)
		now2 := time.Now()
		if now2.After(entry.until) {
			state.Delete(key)
			return req, nil, nil
		}
	}
	nowErr := providerError(key)
	ctx.Log(schemas.LogLevelWarn, nowErr.Error.Message)
	ctx.SetValue("provider-circuit-breaker:skipped", true)
	return req, &schemas.LLMPluginShortCircuit{Error: &nowErr}, nil
}

func PostLLMHook(ctx *schemas.BifrostContext, resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if ctx.Value("provider-circuit-breaker:skipped") != nil {
		return resp, bifrostErr, nil
	}
	if bifrostErr == nil {
		key, _ := ctx.Value(providerContextKey).(string)
		if key != "" {
			ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("%s succeeded", key))
		}
		return resp, nil, nil
	}
	if !isLivenessFailure(bifrostErr) {
		return resp, bifrostErr, nil
	}
	key := providerKeyFromResponse(resp, bifrostErr)
	if key == "" {
		if k, ok := ctx.Value(providerContextKey).(string); ok {
			key = k
		}
	}
	if key == "" {
		return resp, bifrostErr, nil
	}
	if ignoreMissing && !isConfigured(providerKey(key)) {
		ctx.Log(schemas.LogLevelWarn, fmt.Sprintf("%s posthook skipped", key))
		return resp, bifrostErr, nil
	}
	markProviderFailed(key)
	ctx.Log(schemas.LogLevelWarn, unavailableMessage(key))
	return resp, bifrostErr, nil
}

func providerKeyFromRequest(req *schemas.BifrostRequest) string {
	if req == nil {
		return ""
	}
	if req.ChatRequest != nil {
		return resolveProviderKey(string(req.ChatRequest.Provider), req.ChatRequest.Model)
	}
	if req.ResponsesRequest != nil {
		return resolveProviderKey(string(req.ResponsesRequest.Provider), req.ResponsesRequest.Model)
	}
	if req.TextCompletionRequest != nil {
		return resolveProviderKey(string(req.TextCompletionRequest.Provider), req.TextCompletionRequest.Model)
	}
	if req.EmbeddingRequest != nil {
		return resolveProviderKey(string(req.EmbeddingRequest.Provider), req.EmbeddingRequest.Model)
	}
	if req.ImageGenerationRequest != nil {
		return resolveProviderKey(string(req.ImageGenerationRequest.Provider), req.ImageGenerationRequest.Model)
	}
	if req.OCRRequest != nil {
		return resolveProviderKey(string(req.OCRRequest.Provider), req.OCRRequest.Model)
	}
	if req.RerankRequest != nil {
		return resolveProviderKey(string(req.RerankRequest.Provider), req.RerankRequest.Model)
	}
	if req.SpeechRequest != nil {
		return resolveProviderKey(string(req.SpeechRequest.Provider), req.SpeechRequest.Model)
	}
	if req.TranscriptionRequest != nil {
		return resolveProviderKey(string(req.TranscriptionRequest.Provider), req.TranscriptionRequest.Model)
	}
	return ""
}

func providerKeyFromResponse(resp *schemas.BifrostResponse, bifrostErr *schemas.BifrostError) string {
	if resp == nil {
		return ""
	}
	provider := ""
	if bifrostErr != nil {
		provider = string(bifrostErr.ExtraFields.Provider)
		if provider == "" {
			provider = string(bifrostErr.ExtraFields.RoutingInfo.Provider)
		}
	}
	if resp.ResponsesResponse != nil {
		return resolveProviderKey(provider, resp.ResponsesResponse.Model)
	}
	if resp.ChatResponse != nil {
		return resolveProviderKey(provider, resp.ChatResponse.Model)
	}
	if resp.TextCompletionResponse != nil {
		return resolveProviderKey(provider, resp.TextCompletionResponse.Model)
	}
	if resp.EmbeddingResponse != nil {
		return resolveProviderKey(provider, resp.EmbeddingResponse.Model)
	}
	if resp.ImageGenerationResponse != nil {
		return resolveProviderKey(provider, resp.ImageGenerationResponse.Model)
	}
	if resp.OCRResponse != nil {
		return resolveProviderKey(provider, resp.OCRResponse.Model)
	}
	if resp.RerankResponse != nil {
		return resolveProviderKey(provider, resp.RerankResponse.Model)
	}

	return ""
}

func resolveProviderKey(provider, model string) string {
	if provider == "" {
		return ""
	}
	if model == "" {
		return provider
	}
	_perModel := perModel
	if pc, ok := providerConfigs[provider]; ok {
		_perModel = pc.PerModel
	}
	if _perModel {
		return provider + "/" + model
	}
	return provider
}

func isConfigured(provider string) bool {
	_, ok := providerConfigs[provider]
	return ok
}

func providerKey(model string) string {
	if i := strings.IndexByte(model, '/'); i >= 0 {
		return model[:i]
	}
	return model
}

func isLivenessFailure(err *schemas.BifrostError) bool {
	if err == nil {
		return false
	}
	if (err.Error != nil && err.Error.Code != nil && *err.Error.Code == "provider_circuit_open") ||
		(err.StatusCode != nil && *err.StatusCode >= 500 && *err.StatusCode <= 599) {
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

func providerHealthy(key string) bool {
	if i := strings.IndexByte(key, '/'); i >= 0 {
		key = key[:i]
	}
	pc, ok := providerConfigs[key]
	if !ok || !pc.Enabled || pc.SkipCheck || pc.URL == "" {
		return true
	}
	timeout := time.Second
	if pc.Timeout > 0 {
		timeout = pc.Timeout
	}
	client := &http.Client{Timeout: timeout}
	client.Transport = &http.Transport{
		DialContext: (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext,
	}
	resp, err := client.Get(pc.URL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func modelFailed(model string) bool {
	if raw, ok := state.Load(model); ok {
		entry := raw.(*providerState)
		if time.Now().Before(entry.until) {
			return true
		}
	}
	i := strings.IndexByte(model, '/')
	if i < 0 {
		return false
	}
	raw, ok := state.Load(model[:i])
	if !ok {
		return false
	}
	entry := raw.(*providerState)
	return time.Now().Before(entry.until)
}

func markProviderFailed(key string) {
	failures := 1
	if raw, ok := state.Load(key); ok {
		failures = raw.(*providerState).failures + 1
	}
	state.Store(key, &providerState{
		until:    time.Now().Add(nextBackoff(baseTTL, maxTTL, failures)),
		failures: failures,
	})
}

func providerError(key string) schemas.BifrostError {
	nowErr := *baseErr
	nowErr.Error.Message = unavailableMessage(key)
	return nowErr
}

func unavailableMessage(key string) string {
	raw, _ := state.Load(key)
	entry := raw.(*providerState)
	return fmt.Sprintf("%s is temporarily unavailable until %s", key, entry.until.UTC().Format(time.RFC3339))
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

func strPtr(v string) *string {
	return &v
}
