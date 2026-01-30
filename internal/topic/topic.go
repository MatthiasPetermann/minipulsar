package topic

import (
	"fmt"
	"strings"
)

const (
	persistentScheme    = "persistent"
	nonPersistentScheme = "non-persistent"
	defaultTenant       = "public"
	defaultNamespace    = "default"
)

// Info describes a parsed Pulsar topic name.
type Info struct {
	Persistent bool
	Tenant     string
	Namespace  string
	Name       string
	FullName   string
}

// Parse extracts scheme, tenant, namespace, and topic name from a Pulsar topic.
// It accepts fully-qualified persistent:// or non-persistent:// names.
// If no scheme is present, it defaults to persistent://public/default/<topic>.
func Parse(raw string) (Info, error) {
	if raw == "" {
		return Info{}, fmt.Errorf("empty topic")
	}

	scheme := persistentScheme
	rest := raw
	if strings.Contains(raw, "://") {
		parts := strings.SplitN(raw, "://", 2)
		scheme = parts[0]
		rest = parts[1]
	}

	switch scheme {
	case persistentScheme, nonPersistentScheme:
	default:
		return Info{}, fmt.Errorf("unsupported topic scheme %q", scheme)
	}

	if rest == "" {
		return Info{}, fmt.Errorf("invalid topic %q", raw)
	}

	var tenant, namespace, name string
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) == 3 {
		tenant, namespace, name = parts[0], parts[1], parts[2]
	} else if strings.Contains(raw, "://") {
		return Info{}, fmt.Errorf("invalid topic %q", raw)
	} else {
		tenant, namespace, name = defaultTenant, defaultNamespace, rest
	}

	if tenant == "" || namespace == "" || name == "" {
		return Info{}, fmt.Errorf("invalid topic %q", raw)
	}

	fullName := fmt.Sprintf("%s://%s/%s/%s", scheme, tenant, namespace, name)

	return Info{
		Persistent: scheme == persistentScheme,
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       name,
		FullName:   fullName,
	}, nil
}
