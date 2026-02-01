package messaging

import "testing"

func TestBuildSecurityIRAndAllows(t *testing.T) {
	// Pulsar authorization can be strict, requiring explicit role allowlists per namespace,
	// so we validate that produces/consumes are enforced against the configured roles.
	cfg := &Config{
		Security: &SecurityConfig{Mode: "strict"},
		Namespaces: []NamespaceConfig{
			{
				Name:    "persistent://public/default",
				Produce: []string{"writer"},
				Consume: []string{"reader"},
			},
		},
	}
	ir, err := BuildSecurityIR(cfg)
	if err != nil {
		t.Fatalf("build security IR: %v", err)
	}
	ns := "persistent://public/default"
	if !ir.Allows(ns, ActionProduce, []string{"writer"}) {
		t.Fatalf("expected produce to be allowed for writer")
	}
	if ir.Allows(ns, ActionConsume, []string{"writer"}) {
		t.Fatalf("expected consume to be denied for writer")
	}
	if ir.Allows(ns, ActionProduce, []string{"unknown"}) {
		t.Fatalf("expected produce to be denied for unknown role")
	}
}

func TestBuildSecurityIRModeOpen(t *testing.T) {
	// Pulsar "open" authorization mode allows access without namespace allowlists,
	// so allows should return true regardless of roles.
	cfg := &Config{Security: &SecurityConfig{Mode: "open"}}
	ir, err := BuildSecurityIR(cfg)
	if err != nil {
		t.Fatalf("build security IR: %v", err)
	}
	if !ir.Allows("persistent://public/default", ActionProduce, []string{"any"}) {
		t.Fatalf("expected open mode to allow produce")
	}
}
