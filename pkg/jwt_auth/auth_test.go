package jwtauth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestValidToken(t *testing.T) {
	var claims Claims
	claims.Username = "saml"
	claims.Password = "saml"
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(24 * time.Hour))

	jwtAuthManager, err := NewJWTAuthManager("../../certs/jwt_private.pem", &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	tokenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	if err != nil {
		t.Fatalf("Failed to create token validator %v", err)
	}

	err = tokenValidator.ValidateToken(jwtAuthManager.Token)
	if err != nil {
		t.Errorf("Failed to validate token %v", err)
	}
}

func TestExpiredToken(t *testing.T) {
	var claims Claims
	claims.Username = "saml"
	claims.Password = "saml"
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(3 * time.Second))

	jwtAuthManager, err := NewJWTAuthManager("../../certs/jwt_private.pem", &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	tokenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	if err != nil {
		t.Fatalf("Failed to create token validator %v", err)
	}

	// wait for the token to expire
	time.Sleep(4 * time.Second)

	err = tokenValidator.ValidateToken(jwtAuthManager.Token)
	if err == nil {
		t.Fatalf("Error is expected %v", err)
	}

	if !strings.Contains(err.Error(), "token is expired") {
		t.Errorf("Token expired expected, got %v", err)
	}
}
