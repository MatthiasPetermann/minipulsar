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

// NamespacePolicy is the explicit allowlist per action.
type NamespacePolicy struct {
	Allowed map[Action]map[string]struct{}
}

// SecurityIR is the explicit, efficient representation of authorization data.
type SecurityIR struct {
	Mode       SecurityMode
	Namespaces map[string]NamespacePolicy
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

	ir := &SecurityIR{
		Mode:       mode,
		Namespaces: make(map[string]NamespacePolicy),
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
	if s.Mode == ModeOpen {
		return true
	}

	nsPolicy, ok := s.Namespaces[namespace]
	if !ok {
		return false
	}

	allowedRoles, ok := nsPolicy.Allowed[action]
	if !ok || len(allowedRoles) == 0 {
		return false
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
