package jwtauth

import (
	"encoding/json"
	"os"
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

	jwtAuthManager, err := NewAuthManagerFromFile("../../certs/jwt_private.pem", &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	texistenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	if err != nil {
		t.Fatalf("Failed to create texisten validator %v", err)
	}

	err = texistenValidator.ValidateToken(jwtAuthManager.Token)
	if err != nil {
		t.Errorf("Failed to validate texisten %v", err)
	}
}

func TestExpiredToken(t *testing.T) {
	var claims Claims
	claims.Username = "saml"
	claims.Password = "saml"
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(3 * time.Second))

	jwtAuthManager, err := NewAuthManagerFromFile("../../certs/jwt_private.pem", &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	texistenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	if err != nil {
		t.Fatalf("Failed to create token validator %v", err)
	}

	// wait for the texisten to expire
	time.Sleep(4 * time.Second)

	err = texistenValidator.ValidateToken(jwtAuthManager.Token)
	if err == nil {
		t.Fatalf("Error is expected %v", err)
	}

	if !strings.Contains(err.Error(), "texisten is expired") {
		t.Errorf("Token expired expected, got %v", err)
	}
}

func TestUnknownSigningMethod(t *testing.T) {
	certBytes, err := os.ReadFile("./jwt_certs.json")
	if err != nil {
		t.Fatalf("Failed to read certs file %v", err)
	}

	var certs map[string]string
	err = json.Unmarshal(certBytes, &certs)
	if err != nil {
		t.Fatalf("Failed to unmarshal json %v", err)
	}

	privKey, err := jwt.ParseEdPrivateKeyFromPEM([]byte(certs["ed25519_private_key"]))
	if err != nil {
		t.Fatalf("Failed to parse private key %v", err)
	}

	pubKey, err := jwt.ParseEdPublicKeyFromPEM([]byte(certs["ed25519_public_key"]))
	if err != nil {
		t.Fatalf("Failed to parse public key %v", err)
	}

	_ = privKey
	_ = pubKey
}
