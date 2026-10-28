package main

import (
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
	if got := providerKeyFromRequest(req); got != "openai" {
		t.Fatalf("expected model key, got %q", got)
	}
	if got := providerKeyFromRequest(nil); got != "" {
		t.Fatalf("expected empty key, got %q", got)
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

func clearState() {
	state.Range(func(k, _ any) bool {
		state.Delete(k)
		return true
	})
}
