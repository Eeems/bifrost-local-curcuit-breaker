package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

func TestNextBackoffGrowsAndCaps(t *testing.T) {
	base := 10 * time.Second
	max := 80 * time.Second
	if got := nextBackoff(base, max, 1); got != 10*time.Second {
		t.Fatalf("expected 10s, got %s", got)
	}
	if got := nextBackoff(base, max, 2); got != 20*time.Second {
		t.Fatalf("expected 20s, got %s", got)
	}
	if got := nextBackoff(base, max, 10); got != 80*time.Second {
		t.Fatalf("expected cap 80s, got %s", got)
	}
	if got := nextBackoff(base, max, 100); got != 80*time.Second {
		t.Fatalf("expected cap 80s, got %s", got)
	}
}

func TestProviderKeyFromRequest(t *testing.T) {
	req := &schemas.BifrostRequest{
		ChatRequest: &schemas.BifrostChatRequest{Model: "openai/gpt-4o"},
	}
	if got := providerKeyFromRequest(req); got != "openai/gpt-4o" {
		t.Fatalf("expected model key, got %q", got)
	}
	if got := providerKeyFromRequest(nil); got != "" {
		t.Fatalf("expected empty key, got %q", got)
	}
}

func TestInitValidation(t *testing.T) {
	if err := Init(map[string]any{}); err == nil {
		t.Fatal("expected error for missing ttl")
	}
	if err := Init(map[string]any{"ttl": "bad", "max_ttl": 2, "providers": map[string]any{}}); err == nil {
		t.Fatal("expected error for invalid ttl")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 0, "providers": map[string]any{}}); err == nil {
		t.Fatal("expected error for invalid max_ttl")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "", "timeout": 1, "per_model": true, "ignore": false, "poll": 1},
	}}); err == nil {
		t.Fatal("expected error for empty url")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "http://x", "timeout": 0, "per_model": true, "ignore": false, "poll": 1},
	}}); err == nil {
		t.Fatal("expected error for bad timeout")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "http://x", "timeout": 1, "per_model": "bad", "ignore": false, "poll": 1},
	}}); err == nil {
		t.Fatal("expected error for bad per_model")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "http://x", "timeout": 1, "per_model": true, "ignore": "bad", "poll": 1},
	}}); err == nil {
		t.Fatal("expected error for bad ignore")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "http://x", "timeout": 1, "per_model": true, "ignore": false, "poll": 0},
	}}); err == nil {
		t.Fatal("expected error for bad poll")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "foo": 1}); err == nil {
		t.Fatal("expected error for unexpected key")
	}
	if err := Init(map[string]any{"ttl": 1, "max_ttl": 2, "providers": map[string]any{
		"p": map[string]any{"url": "http://x", "timeout": 1, "per_model": true, "ignore": false, "poll": 1},
	}}); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if len(providerConfigs) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providerConfigs))
	}
	pc := providerConfigs["p"]
	if pc.URL != "http://x" || pc.Timeout != 1 || !pc.PerModel || pc.Ignore || pc.Poll != 1 {
		t.Fatalf("unexpected provider config: %+v", pc)
	}
}

func TestDurationFromConfig(t *testing.T) {
	if got := durationFromConfig(map[string]any{"x": 5}, "x"); got != 5*time.Second {
		t.Fatalf("expected 5s, got %s", got)
	}
	if got := durationFromConfig(map[string]any{"x": float64(5)}, "x"); got != 5*time.Second {
		t.Fatalf("expected 5s, got %s", got)
	}
	if got := durationFromConfig(map[string]any{"x": "bad"}, "x"); got != 0 {
		t.Fatalf("expected 0, got %s", got)
	}
	if got := durationFromConfig(map[string]any{}, "x"); got != 0 {
		t.Fatalf("expected 0, got %s", got)
	}
}

func TestIsLivenessFailure(t *testing.T) {
	status := 503
	err := &schemas.BifrostError{
		StatusCode: &status,
		Error: &schemas.ErrorField{
			Code: strPtr("provider_error"),
		},
	}
	if !isLivenessFailure(err) {
		t.Fatal("expected 503 to be a liveness failure")
	}

	clientStatus := 400
	clientErr := &schemas.BifrostError{
		StatusCode: &clientStatus,
		Error: &schemas.ErrorField{
			Code: strPtr("bad_request"),
		},
	}
	if isLivenessFailure(clientErr) {
		t.Fatal("did not expect 400 to be a liveness failure")
	}
}

func TestPostLLMMarksProvider(t *testing.T) {
	clearState()
	status := 500
	bifrostErr := &schemas.BifrostError{
		StatusCode:  &status,
		ExtraFields: schemas.BifrostErrorExtraFields{Provider: "openai/gpt-4o"},
		Error: &schemas.ErrorField{
			Code: strPtr("provider_error"),
		},
	}
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	ctx.SetValue(providerContextKey, "openai/gpt-4o")

	PostLLMHook(ctx, nil, bifrostErr)
	if _, ok := state.Load("openai/gpt-4o"); !ok {
		t.Fatal("expected provider to be marked")
	}
	clearState()
}

func TestProviderHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	providerConfigs = map[string]providerHealthConfig{
		"p": {URL: srv.URL, Timeout: 1},
	}
	if !providerHealthy("p") {
		t.Fatal("expected healthy")
	}
	providerConfigs["p"] = providerHealthConfig{URL: srv.URL, Ignore: true}
	if !providerHealthy("p") {
		t.Fatal("ignored should be healthy")
	}
	providerConfigs = map[string]providerHealthConfig{
		"p": {URL: "http://127.0.0.1:1", Timeout: 1},
	}
	if providerHealthy("p") {
		t.Fatal("expected unhealthy for unreachable")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1002 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()
	providerConfigs = map[string]providerHealthConfig{
		"p": {URL: slow.URL, Timeout: 1},
	}
	if providerHealthy("p") {
		t.Fatal("expected timeout to mark unhealthy")
	}
	providerConfigs = map[string]providerHealthConfig{
		"p": {URL: slow.URL, Timeout: 3},
	}
	if !providerHealthy("p") {
		t.Fatal("expected larger timeout to succeed")
	}
	providerConfigs = nil
}

func TestMarkProviderFailed(t *testing.T) {
	clearState()
	markProviderFailed("p")
	if _, ok := state.Load("p"); !ok {
		t.Fatal("expected state")
	}
	markProviderFailed("p")
	raw, _ := state.Load("p")
	if raw.(*providerState).failures != 2 {
		t.Fatalf("expected 2 failures, got %d", raw.(*providerState).failures)
	}
	clearState()
}

func TestPreLLMHookShortCircuits(t *testing.T) {
	clearState()
	state.Store("openai/gpt-4o", &providerState{until: time.Now().Add(time.Minute), failures: 1})
	ctx := schemas.NewBifrostContext(nil, time.Time{})
	req := &schemas.BifrostRequest{ChatRequest: &schemas.BifrostChatRequest{Model: "openai/gpt-4o"}}
	_, sc, err := PreLLMHook(ctx, req)
	if err != nil || sc == nil {
		t.Fatalf("expected short circuit, got err=%v sc=%v", err, sc)
	}
	clearState()
}
func clearState() {
	state.Range(func(k, _ any) bool {
		state.Delete(k)
		return true
	})
}
