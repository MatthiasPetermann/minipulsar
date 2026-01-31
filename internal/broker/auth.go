package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	pulsar "minipulsar/pb"
)

type jwtClaims struct {
	Roles interface{} `json:"roles"`
	Role  string      `json:"role"`
}

func rolesFromConnect(cmd *pulsar.CommandConnect) ([]string, error) {
	if cmd == nil {
		return nil, nil
	}
	data := cmd.GetAuthData()
	if len(data) == 0 {
		if original := cmd.GetOriginalAuthData(); original != "" {
			data = []byte(original)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return nil, nil
	}
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid jwt token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode jwt payload: %w", err)
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal jwt payload: %w", err)
	}
	roles := extractRoles(claims)
	return roles, nil
}

func extractRoles(claims jwtClaims) []string {
	if claims.Role != "" {
		return []string{claims.Role}
	}
	switch value := claims.Roles.(type) {
	case []interface{}:
		roles := make([]string, 0, len(value))
		for _, item := range value {
			if role, ok := item.(string); ok && role != "" {
				roles = append(roles, role)
			}
		}
		return roles
	case []string:
		roles := make([]string, 0, len(value))
		for _, role := range value {
			if role != "" {
				roles = append(roles, role)
			}
		}
		return roles
	case string:
		if value == "" {
			return nil
		}
		return []string{value}
	default:
		return nil
	}
}
