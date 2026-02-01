package messaging

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRequiresPath(t *testing.T) {
	// Pulsar control-plane config must be explicitly provided to avoid ambiguous defaults,
	// so loading with an empty path should fail fast.
	if _, err := LoadConfig(""); err == nil {
		t.Fatalf("expected error for empty config path")
	}
}

func TestLoadConfigParsesHCL(t *testing.T) {
	// The messaging config defines namespaces, security mode, and bindings,
	// so we verify HCL is decoded into the corresponding structs.
	dir := t.TempDir()
	path := filepath.Join(dir, "config.hcl")
	content := `
security {
  mode = "strict"
}

namespace "persistent://public/default" {
  produce = ["writer"]
  consume = ["reader"]
}

function "uppercase" {
  path = "noop.lua"
  max_runtime = "2s"
}

binding {
  source = "persistent://public/default/input"
  function = "uppercase"
  target = "persistent://public/default/output"
}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Security == nil || cfg.Security.Mode != "strict" {
		t.Fatalf("unexpected security config: %+v", cfg.Security)
	}
	if len(cfg.Namespaces) != 1 || cfg.Namespaces[0].Name != "persistent://public/default" {
		t.Fatalf("unexpected namespaces: %+v", cfg.Namespaces)
	}
	if len(cfg.Functions) != 1 || cfg.Functions[0].ID != "uppercase" {
		t.Fatalf("unexpected functions: %+v", cfg.Functions)
	}
	if len(cfg.Bindings) != 1 || cfg.Bindings[0].Source == "" {
		t.Fatalf("unexpected bindings: %+v", cfg.Bindings)
	}
}
