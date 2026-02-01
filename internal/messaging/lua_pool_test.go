package messaging

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFunctionPoolExecute(t *testing.T) {
	// Pulsar function processing expects the Lua handle() entrypoint to transform payloads,
	// so we run a simple Lua function and verify the outgoing payload matches the return value.
	dir := t.TempDir()
	fnPath := filepath.Join(dir, "upper.lua")
	if err := os.WriteFile(fnPath, []byte(`function handle(payload, ctx) return string.upper(payload) end`), 0o600); err != nil {
		t.Fatalf("write function: %v", err)
	}
	registry := &FunctionRegistry{
		Functions: map[string]FunctionSpec{
			"upper": {ID: "upper", Path: fnPath},
		},
	}
	pool, err := NewFunctionPool(registry, 1, nil)
	if err != nil {
		t.Fatalf("new function pool: %v", err)
	}
	out, err := pool.Execute("upper", []byte("pulsar"), FunctionContext{
		FunctionID:  "upper",
		SourceTopic: "persistent://public/default/input",
		TargetTopic: "persistent://public/default/output",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if string(out) != "PULSAR" {
		t.Fatalf("unexpected output: %s", string(out))
	}
}

func TestFunctionPoolRejectsUnknownFunction(t *testing.T) {
	// Pulsar function invocations must reference a registered function id,
	// so unknown IDs should return an error rather than executing.
	registry := &FunctionRegistry{Functions: map[string]FunctionSpec{}}
	pool, err := NewFunctionPool(registry, 1, nil)
	if err != nil {
		t.Fatalf("new function pool: %v", err)
	}
	_, err = pool.Execute("missing", []byte("payload"), FunctionContext{})
	if err == nil {
		t.Fatalf("expected error for unknown function")
	}
}

func TestValidateLuaFunctionRequiresHandle(t *testing.T) {
	// Pulsar functions must expose a handle() entrypoint,
	// so validation should fail if the Lua file lacks it.
	dir := t.TempDir()
	fnPath := filepath.Join(dir, "invalid.lua")
	if err := os.WriteFile(fnPath, []byte(`return 42`), 0o600); err != nil {
		t.Fatalf("write function: %v", err)
	}
	if err := validateLuaFunction(fnPath); err == nil {
		t.Fatalf("expected validation error for missing handle")
	}
}
