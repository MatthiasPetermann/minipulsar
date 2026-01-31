package broker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pulsar "minipulsar/pb"
)

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type jwtClaims struct {
	Roles interface{} `json:"roles"`
	Role  string      `json:"role"`
	Exp   *int64      `json:"exp"`
}

func rolesFromConnect(cmd *pulsar.CommandConnect, secret []byte) ([]string, error) {
	if cmd == nil {
		return nil, nil
	}
	if len(secret) == 0 {
		return nil, fmt.Errorf("jwt secret not configured")
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
	if len(secret) == 0 {
		return nil, fmt.Errorf("jwt secret not configured")
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return nil, nil
	}
	if len(secret) == 0 {
		return nil, fmt.Errorf("jwt secret not configured")
	}
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid jwt token")
	}
	if len(parts) != 3 {
		return nil, fmt.Errorf("jwt token missing signature")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decode jwt header: %w", err)
	}
	var parsedHeader jwtHeader
	if err := json.Unmarshal(header, &parsedHeader); err != nil {
		return nil, fmt.Errorf("unmarshal jwt header: %w", err)
	}
	if strings.ToUpper(parsedHeader.Alg) != "HS256" {
		return nil, fmt.Errorf("unsupported jwt alg %q", parsedHeader.Alg)
	}
	if err := verifyHS256(parts[0], parts[1], parts[2], secret); err != nil {
		return nil, err
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode jwt payload: %w", err)
	}
	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal jwt payload: %w", err)
	}
	if claims.Exp != nil && time.Now().UTC().Unix() >= *claims.Exp {
		return nil, fmt.Errorf("jwt token expired")
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

func verifyHS256(encodedHeader, encodedPayload, encodedSignature string, secret []byte) error {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(encodedHeader))
	_, _ = mac.Write([]byte("."))
	_, _ = mac.Write([]byte(encodedPayload))
	expected := mac.Sum(nil)

	signature, err := base64.RawURLEncoding.DecodeString(encodedSignature)
	if err != nil {
		return fmt.Errorf("decode jwt signature: %w", err)
	}
	if !hmac.Equal(signature, expected) {
		return fmt.Errorf("invalid jwt signature")
	}
	return nil
}
