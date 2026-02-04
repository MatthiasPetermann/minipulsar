package messaging

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"minipulsar/internal/logging"
	"minipulsar/internal/topic"
)

var functionIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// Binding represents a compiled routing binding.
type Binding struct {
	SourceTopic string
	FunctionID  string
	TargetTopic string
}

// FunctionSpec captures validated function metadata.
type FunctionSpec struct {
	ID         string
	Path       string
	MaxRuntime time.Duration
}

// FunctionRegistry holds validated function metadata.
type FunctionRegistry struct {
	Functions map[string]FunctionSpec
}

// Runtime carries the parsed configuration and execution helpers.
type Runtime struct {
	Security          *SecurityIR
	Functions         *FunctionRegistry
	Bindings          map[string][]Binding
	NamespacePolicies map[string]NamespacePolicy
	Pool              *FunctionPool
	Logger            *logging.Logger
}

// Options configures runtime creation.
type Options struct {
	Logger        *logging.Logger
	WorkerCount   int
	ValidateFuncs bool
}

// BuildRuntime validates the config and prepares runtime helpers.
func BuildRuntime(cfg *Config, opts Options) (*Runtime, error) {
	if cfg == nil {
		return nil, nil
	}
	logger := opts.Logger
	if logger == nil {
		defaultLogger, err := logging.New(logging.Options{
			Format:        "text",
			WithTimestamp: true,
			Level:         slog.LevelInfo,
			Writer:        os.Stdout,
		})
		if err == nil {
			logger = defaultLogger.With("component", "messaging")
		}
	}
	securityIR, err := BuildSecurityIR(cfg)
	if err != nil {
		return nil, err
	}

	namespacePolicies := make(map[string]NamespacePolicy)
	for _, ns := range cfg.Namespaces {
		if ns.Name == "" {
			return nil, fmt.Errorf("namespace name is required")
		}
		normalized, err := normalizeNamespace(ns.Name)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", ns.Name, err)
		}
		policy := NamespacePolicy{}
		if ns.SubscriptionTimeoutSeconds < 0 {
			return nil, fmt.Errorf("namespace %q subscription_timeout_seconds must be >= 0", ns.Name)
		}
		if ns.SubscriptionTimeoutSeconds > 0 {
			policy.SubscriptionTimeout = time.Duration(ns.SubscriptionTimeoutSeconds) * time.Second
		}
		if ns.RetentionSeconds < 0 {
			return nil, fmt.Errorf("namespace %q retention_seconds must be >= 0", ns.Name)
		}
		if ns.RetentionSeconds > 0 {
			policy.Retention = time.Duration(ns.RetentionSeconds) * time.Second
		}
		namespacePolicies[normalized] = policy
	}

	registry := &FunctionRegistry{Functions: make(map[string]FunctionSpec)}
	for _, fn := range cfg.Functions {
		if fn.ID == "" {
			return nil, fmt.Errorf("function id is required")
		}
		if !functionIDPattern.MatchString(fn.ID) {
			return nil, fmt.Errorf("function id %q must be alphanumeric with dashes", fn.ID)
		}
		if fn.Path == "" {
			return nil, fmt.Errorf("function %q missing path", fn.ID)
		}
		if _, err := os.Stat(fn.Path); err != nil {
			return nil, fmt.Errorf("function %q path error: %w", fn.ID, err)
		}
		if _, exists := registry.Functions[fn.ID]; exists {
			return nil, fmt.Errorf("duplicate function id %q", fn.ID)
		}
		var maxRuntime time.Duration
		if strings.TrimSpace(fn.MaxRuntime) != "" {
			parsed, err := time.ParseDuration(fn.MaxRuntime)
			if err != nil {
				return nil, fmt.Errorf("function %q invalid max_runtime: %w", fn.ID, err)
			}
			if parsed <= 0 {
				return nil, fmt.Errorf("function %q max_runtime must be positive", fn.ID)
			}
			maxRuntime = parsed
		}
		registry.Functions[fn.ID] = FunctionSpec{
			ID:         fn.ID,
			Path:       fn.Path,
			MaxRuntime: maxRuntime,
		}
	}

	bindings := make(map[string][]Binding)
	for _, binding := range cfg.Bindings {
		if binding.Source == "" || binding.Target == "" || binding.FunctionID == "" {
			return nil, fmt.Errorf("binding requires source, target, and function")
		}
		if _, ok := registry.Functions[binding.FunctionID]; !ok {
			return nil, fmt.Errorf("binding references unknown function %q", binding.FunctionID)
		}
		sourceInfo, err := topic.Parse(binding.Source)
		if err != nil {
			return nil, fmt.Errorf("binding source %q invalid: %w", binding.Source, err)
		}
		targetInfo, err := topic.Parse(binding.Target)
		if err != nil {
			return nil, fmt.Errorf("binding target %q invalid: %w", binding.Target, err)
		}
		bindingEntry := Binding{
			SourceTopic: sourceInfo.FullName,
			FunctionID:  binding.FunctionID,
			TargetTopic: targetInfo.FullName,
		}
		bindings[sourceInfo.FullName] = append(bindings[sourceInfo.FullName], bindingEntry)
	}

	var pool *FunctionPool
	if len(registry.Functions) > 0 {
		workers := opts.WorkerCount
		if workers <= 0 {
			workers = 4
		}
		pool, err = NewFunctionPool(registry, workers, logger)
		if err != nil {
			return nil, err
		}
	}

	runtime := &Runtime{
		Security:          securityIR,
		Functions:         registry,
		Bindings:          bindings,
		NamespacePolicies: namespacePolicies,
		Pool:              pool,
		Logger:            logger,
	}

	if opts.ValidateFuncs {
		if err := runtime.ValidateFunctions(); err != nil {
			return nil, err
		}
	}

	return runtime, nil
}

// NamespacePolicy stores retention and subscription behavior for a namespace.
type NamespacePolicy struct {
	SubscriptionTimeout time.Duration
	Retention           time.Duration
}

// PolicyForTopic returns the namespace policy for a topic, if configured.
func (r *Runtime) PolicyForTopic(info topic.Info) (NamespacePolicy, bool) {
	if r == nil || len(r.NamespacePolicies) == 0 {
		return NamespacePolicy{}, false
	}
	scheme := "persistent"
	if !info.Persistent {
		scheme = "non-persistent"
	}
	key := fmt.Sprintf("%s://%s/%s", scheme, info.Tenant, info.Namespace)
	policy, ok := r.NamespacePolicies[key]
	return policy, ok
}

// BindingsFor returns bindings for a source topic.
func (r *Runtime) BindingsFor(sourceTopic string) []Binding {
	if r == nil {
		return nil
	}
	return r.Bindings[sourceTopic]
}

// ValidateFunctions loads each function and checks for the entrypoint.
func (r *Runtime) ValidateFunctions() error {
	if r == nil || r.Functions == nil {
		return nil
	}
	for _, fn := range r.Functions.Functions {
		if err := validateLuaFunction(fn.Path); err != nil {
			return fmt.Errorf("function %q invalid: %w", fn.ID, err)
		}
	}
	return nil
}

// normalizeNamespace validates and canonicalizes a namespace string.
func normalizeNamespace(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("namespace is empty")
	}
	if !strings.Contains(raw, "://") {
		return "", fmt.Errorf("namespace %q must include scheme", raw)
	}
	info, err := topic.Parse(raw + "/__validate")
	if err != nil {
		return "", err
	}
	scheme := "persistent"
	if !info.Persistent {
		scheme = "non-persistent"
	}
	return fmt.Sprintf("%s://%s/%s", scheme, info.Tenant, info.Namespace), nil
}
