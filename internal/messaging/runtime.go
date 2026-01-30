package messaging

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"

	"minipulsar/internal/topic"
)

var functionIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

// Binding represents a compiled routing binding.
type Binding struct {
	SourceTopic string
	FunctionID  string
	TargetTopic string
}

// FunctionRegistry holds validated function metadata.
type FunctionRegistry struct {
	Functions map[string]FunctionConfig
}

// Runtime carries the parsed configuration and execution helpers.
type Runtime struct {
	Security  *SecurityIR
	Functions *FunctionRegistry
	Bindings  map[string][]Binding
	Pool      *FunctionPool
	Logger    *logrus.Entry
}

// Options configures runtime creation.
type Options struct {
	Logger        *logrus.Entry
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
		logger = logrus.New().WithField("component", "messaging")
	}
	securityIR, err := BuildSecurityIR(cfg)
	if err != nil {
		return nil, err
	}

	registry := &FunctionRegistry{Functions: make(map[string]FunctionConfig)}
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
		registry.Functions[fn.ID] = fn
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
		Security:  securityIR,
		Functions: registry,
		Bindings:  bindings,
		Pool:      pool,
		Logger:    logger,
	}

	if opts.ValidateFuncs {
		if err := runtime.ValidateFunctions(); err != nil {
			return nil, err
		}
	}

	return runtime, nil
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
