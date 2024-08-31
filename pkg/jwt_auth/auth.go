package jwtauth

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/golang-jwt/jwt/v5"

	"github.com/isnastish/kvs/pkg/log"
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

type Claims struct {
	// We use username and password, but it could be anything,
	// for example a project name, namespace etc.
	Username string `json:"user,omitempty"`
	Password string `json:"pwd,omitempty"`

	jwt.RegisteredClaims
}

type JWTAuthManager struct {
	Token string
}

func NewJWTAuthManager(privateKeyPath string, claims *Claims) (*JWTAuthManager, error) {
	jwtPrivateKeyContents, err := os.ReadFile(privateKeyPath)
	if err != nil {
		log.Logger.Fatal("Failed to read jwt private key file %v", err)
	}

	// NOTE: Instead of parsing manually, jwt could parse a private key for us.
	//	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(jwtPrivateKeyContents)
	//	if err != nil {
	//		log.Logger.Fatal("Failed to parse private key %v", err)
	//	}
	//

	var key crypto.PrivateKey
	pemBlock, _ := pem.Decode(jwtPrivateKeyContents)
	log.Logger.Info("Private key type: %s", pemBlock.Type)
	if pemBlock.Type == "RSA PRIVATE KEY" { // encrypted key
		key, err = x509.ParsePKCS1PrivateKey(pemBlock.Bytes)
		if err != nil {
			log.Logger.Fatal("Failed to parse encrypted jwt private key %s", err)
		}
	} else if pemBlock.Type == "PRIVATE KEY" { // unencrypted key
		key, err = x509.ParsePKCS8PrivateKey(pemBlock.Bytes)
		if err != nil {
			log.Logger.Fatal("Failed to parse unencrypted jwt private key %v", err)
		}
	}

	jwtToken := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token, err := jwtToken.SignedString(key)
	if err != nil {
		log.Logger.Fatal("Failed to create signed jwt token %v", err)
	}

	return &JWTAuthManager{Token: token}, nil
}

func (b JWTAuthManager) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + b.Token,
	}, nil
}

func (b JWTAuthManager) RequireTransportSecurity() bool {
	return true
}
