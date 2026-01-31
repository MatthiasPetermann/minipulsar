package messaging

import (
	"fmt"
	"strings"
)

// Action represents a security decision type.
type Action string

const (
	ActionProduce Action = "produce"
	ActionConsume Action = "consume"
)

// SecurityMode controls how missing namespaces are handled.
type SecurityMode string

const (
	ModeStrict SecurityMode = "strict"
	ModeOpen   SecurityMode = "open"
)

// Policy determines default allow/deny behavior.
type Policy string

const (
	PolicyAllow Policy = "allow"
	PolicyDeny  Policy = "deny"
)

// NamespacePolicy is the explicit allowlist per action.
type NamespacePolicy struct {
	Allowed map[Action]map[string]struct{}
}

// SecurityIR is the explicit, efficient representation of authorization data.
type SecurityIR struct {
	Mode          SecurityMode
	DefaultPolicy Policy
	Namespaces    map[string]NamespacePolicy
}

// BuildSecurityIR validates and converts config into the runtime IR.
func BuildSecurityIR(cfg *Config) (*SecurityIR, error) {
	if cfg == nil || cfg.Security == nil {
		return nil, nil
	}

	mode := SecurityMode(strings.ToLower(strings.TrimSpace(cfg.Security.Mode)))
	if mode == "" {
		return nil, fmt.Errorf("security mode is required")
	}
	switch mode {
	case ModeStrict, ModeOpen:
	default:
		return nil, fmt.Errorf("unsupported security mode %q", cfg.Security.Mode)
	}

	policy := Policy(strings.ToLower(strings.TrimSpace(cfg.Security.DefaultPolicy)))
	if policy == "" {
		return nil, fmt.Errorf("security default_policy is required")
	}
	switch policy {
	case PolicyAllow, PolicyDeny:
	default:
		return nil, fmt.Errorf("unsupported security default_policy %q", cfg.Security.DefaultPolicy)
	}

	ir := &SecurityIR{
		Mode:          mode,
		DefaultPolicy: policy,
		Namespaces:    make(map[string]NamespacePolicy),
	}

	for _, ns := range cfg.Namespaces {
		if ns.Name == "" {
			return nil, fmt.Errorf("namespace name is required")
		}
		normalized, err := normalizeNamespace(ns.Name)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", ns.Name, err)
		}
		policyEntry := NamespacePolicy{Allowed: make(map[Action]map[string]struct{})}
		if len(ns.Produce) > 0 {
			policyEntry.Allowed[ActionProduce] = toRoleSet(ns.Produce)
		}
		if len(ns.Consume) > 0 {
			policyEntry.Allowed[ActionConsume] = toRoleSet(ns.Consume)
		}
		ir.Namespaces[normalized] = policyEntry
	}

	return ir, nil
}

// Allows evaluates a decision using the IR.
func (s *SecurityIR) Allows(namespace string, action Action, roles []string) bool {
	if s == nil {
		return true
	}
	if namespace == "" {
		return false
	}

	nsPolicy, ok := s.Namespaces[namespace]
	if !ok {
		if s.Mode == ModeStrict {
			return false
		}
		return s.DefaultPolicy == PolicyAllow
	}

	allowedRoles, ok := nsPolicy.Allowed[action]
	if !ok || len(allowedRoles) == 0 {
		return s.DefaultPolicy == PolicyAllow
	}

	for _, role := range roles {
		if _, ok := allowedRoles[role]; ok {
			return true
		}
	}
	return false
}

func toRoleSet(roles []string) map[string]struct{} {
	set := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		if role == "" {
			continue
		}
		set[role] = struct{}{}
	}
	return set
}
