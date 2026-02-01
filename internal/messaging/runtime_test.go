package messaging

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"minipulsar/internal/topic"
)

func TestBuildRuntimeValidatesBindingsAndPolicies(t *testing.T) {
	// Pulsar bindings rely on valid topic names and namespace policies,
	// so we ensure runtime construction normalizes namespaces and compiles bindings.
	dir := t.TempDir()
	fnPath := filepath.Join(dir, "noop.lua")
	if err := os.WriteFile(fnPath, []byte(`function handle(payload, ctx) return payload end`), 0o600); err != nil {
		t.Fatalf("write function: %v", err)
	}
	cfg := &Config{
		Security: &SecurityConfig{Mode: "open"},
		Namespaces: []NamespaceConfig{
			{
				Name:                       "persistent://public/default",
				SubscriptionTimeoutSeconds: 30,
				RetentionSeconds:           60,
			},
		},
		Functions: []FunctionConfig{
			{ID: "noop", Path: fnPath, MaxRuntime: "1s"},
		},
		Bindings: []BindingConfig{
			{
				Source:     "persistent://public/default/input",
				FunctionID: "noop",
				Target:     "persistent://public/default/output",
			},
		},
	}

	runtime, err := BuildRuntime(cfg, Options{ValidateFuncs: true})
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}
	bindings := runtime.BindingsFor("persistent://public/default/input")
	if len(bindings) != 1 {
		t.Fatalf("unexpected binding count: %d", len(bindings))
	}
	info, err := topic.Parse("persistent://public/default/input")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	policy, ok := runtime.PolicyForTopic(info)
	if !ok {
		t.Fatalf("expected namespace policy")
	}
	if policy.SubscriptionTimeout != 30*time.Second || policy.Retention != 60*time.Second {
		t.Fatalf("unexpected policy: %+v", policy)
	}
}

func TestNormalizeNamespaceErrors(t *testing.T) {
	// Pulsar namespace identifiers must include a scheme and tenant/namespace,
	// so normalization should reject empty or scheme-less values.
	if _, err := normalizeNamespace(""); err == nil {
		t.Fatalf("expected error for empty namespace")
	}
	if _, err := normalizeNamespace("public/default"); err == nil {
		t.Fatalf("expected error for missing scheme")
	}
}
