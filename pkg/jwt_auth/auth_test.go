package jwtauth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var certs map[string]string

func TestMain(m *testing.M) {
	certBytes, err := os.ReadFile("jwt_test_certs.json")
	if err != nil {
		fmt.Printf("Failed to read certificates file %v\n", err)
		os.Exit(1)
	}

	err = json.Unmarshal(certBytes, &certs)
	if err != nil {
		fmt.Printf("Failed to unmarshal json %v\n", err)
		os.Exit(1)
	}

	status := m.Run()

	os.Exit(status)
}

func TestValidToken(t *testing.T) {
	var claims Claims
	claims.Username = "saml"
	claims.Password = "saml"
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(24 * time.Hour))

	jwtAuthManager, err := NewAuthManagerFromBytes([]byte(certs["rsa_private_key"]), &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	texistenValidator, err := NewTokenValidatorFromBytes([]byte(certs["rsa_public_key"]))
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

	jwtAuthManager, err := NewAuthManagerFromBytes([]byte(certs["rsa_private_key"]), &claims)
	if err != nil {
		t.Fatalf("Failed to create auth manager %v", err)
	}

	texistenValidator, err := NewTokenValidatorFromBytes([]byte(certs["rsa_public_key"]))
	if err != nil {
		t.Fatalf("Failed to create token validator %v", err)
	}

	// wait for the texisten to expire
	time.Sleep(4 * time.Second)

	err = texistenValidator.ValidateToken(jwtAuthManager.Token)
	if err == nil {
		t.Fatalf("Error is expected %v", err)
	}

	if !strings.Contains(err.Error(), "token is expired") {
		t.Errorf("Token expired expected, got %v", err)
	}
}

func TestUnknownSigningMethod(t *testing.T) {
	privKey, err := jwt.ParseEdPrivateKeyFromPEM([]byte(certs["ed25519_private_key"]))
	if err != nil {
		t.Fatalf("Failed to parse private key %v", err)
	}

	var claims Claims
	claims.Username = "saml"
	claims.Password = "saml"
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(24 * time.Hour))

	jwtToken := jwt.NewWithClaims(&jwt.SigningMethodEd25519{}, claims)
	token, err := jwtToken.SignedString(privKey)
	if err != nil {
		t.Fatalf("Failed to sign jwt token %v", err)
	}

	// Token validator expects a single method for signing keys (rsa256)
	// Here, the key was signed with a different algorithm, thus the token validation should fail.
	tokenValidator := JWTValidator{}
	err = tokenValidator.ValidateToken(token)

	if !strings.Contains(err.Error(), "unexpected signing method") {
		t.Errorf("Error mismatch")
	}
}
