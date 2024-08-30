package jwtauth

import (
	"crypto"
	"fmt"
	"os"

	"github.com/golang-jwt/jwt/v5"
)

// TODO: Try out ED25519 instead of RSA for generating private/public key pairs.
// The generated keys are usually smaller and more secure.
// openssl genpkey -algorithm ED25519 ...

const AuthBearerPrefix = "Bearer "

type JWTValidator struct {
	key crypto.PublicKey
}

func NewTokenValidator(publicKeyPath string) (*JWTValidator, error) {
	keyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key file %v", err)
	}
	pubKey, err := jwt.ParseRSAPublicKeyFromPEM(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key %v", err)
	}

	return &JWTValidator{key: pubKey}, nil
}

func (v *JWTValidator) ValidateToken(tokenString string) error {
	_, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Figure out whether the token came from somebody we trust.
		// check whether a token uses expected signing method.
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}

		// Return a single possible key we trust.
		return v.key, nil
	})

	if err != nil {
		return fmt.Errorf("failed to parse token %v", err)
	}

	return nil
}
