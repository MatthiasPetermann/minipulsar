package topic

import "testing"

func TestParseDefaultsToPersistent(t *testing.T) {
	// Pulsar allows shorthand topic names that default to persistent://public/default,
	// so we confirm the parser fills in that canonical scheme and namespace.
	info, err := Parse("orders")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	if !info.Persistent {
		t.Fatalf("expected persistent topic")
	}
	if info.Tenant != "public" || info.Namespace != "default" || info.Name != "orders" {
		t.Fatalf("unexpected default parts: %+v", info)
	}
	if info.FullName != "persistent://public/default/orders" {
		t.Fatalf("unexpected full name: %s", info.FullName)
	}
}

func TestParseQualifiedPersistent(t *testing.T) {
	// Pulsar protocol requires fully-qualified names when a scheme is provided,
	// so we ensure tenant/namespace/topic are preserved for persistent topics.
	info, err := Parse("persistent://acme/analytics/events")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	if !info.Persistent {
		t.Fatalf("expected persistent topic")
	}
	if info.Tenant != "acme" || info.Namespace != "analytics" || info.Name != "events" {
		t.Fatalf("unexpected parts: %+v", info)
	}
	if info.FullName != "persistent://acme/analytics/events" {
		t.Fatalf("unexpected full name: %s", info.FullName)
	}
}

func TestParseQualifiedNonPersistent(t *testing.T) {
	// Pulsar distinguishes non-persistent topics via the non-persistent:// scheme,
	// so we verify the parser flags them as non-persistent with the right full name.
	info, err := Parse("non-persistent://acme/stream/ephemeral")
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}
	if info.Persistent {
		t.Fatalf("expected non-persistent topic")
	}
	if info.FullName != "non-persistent://acme/stream/ephemeral" {
		t.Fatalf("unexpected full name: %s", info.FullName)
	}
}

func TestParseRejectsInvalid(t *testing.T) {
	// Pulsar clients must send valid topic strings, so we reject empty or malformed names.
	_, err := Parse("")
	if err == nil {
		t.Fatalf("expected error for empty topic")
	}
	_, err = Parse("persistent://missing-parts")
	if err == nil {
		t.Fatalf("expected error for malformed qualified topic")
	}
	_, err = Parse("unknown://tenant/ns/topic")
	if err == nil {
		t.Fatalf("expected error for unsupported scheme")
	}
}
