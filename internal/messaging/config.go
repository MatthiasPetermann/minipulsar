package messaging

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/hclsimple"
)

// Config captures the declarative messaging control-plane configuration.
type Config struct {
	Security   *SecurityConfig   `hcl:"security,block"`
	Namespaces []NamespaceConfig `hcl:"namespace,block"`
	Functions  []FunctionConfig  `hcl:"function,block"`
	Bindings   []BindingConfig   `hcl:"binding,block"`
}

// SecurityConfig defines the default authorization behavior.
type SecurityConfig struct {
	Mode string `hcl:"mode"`
}

// NamespaceConfig binds a namespace to allowed roles for produce/consume actions.
type NamespaceConfig struct {
	Name    string   `hcl:"name,label"`
	Produce []string `hcl:"produce,optional"`
	Consume []string `hcl:"consume,optional"`
}

// FunctionConfig defines a named Lua function stored on disk.
type FunctionConfig struct {
	ID         string `hcl:"id,label"`
	Path       string `hcl:"path"`
	MaxRuntime string `hcl:"max_runtime,optional"`
}

// BindingConfig routes a source topic through a Lua function into a target topic.
type BindingConfig struct {
	Source     string `hcl:"source"`
	FunctionID string `hcl:"function"`
	Target     string `hcl:"target"`
}

// LoadConfig reads and decodes the HCL messaging configuration from disk.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		return nil, fmt.Errorf("messaging config path is empty")
	}
	var cfg Config
	if err := hclsimple.DecodeFile(path, nil, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
