package main

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	_ "encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	jwtauth "github.com/isnastish/kvs/pkg/jwt_auth"
	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	var settings kvs.ServiceSettings

	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.BoolVar(&settings.TransactionsDisabled, "txn_disabled", false, "Disable transactions. Enabled by default")
	logLevel := flag.String("log_level", "info", "Log level")
	txnPort := flag.Uint("txn_port", 5051, "Transaction service listening port")
	clientPrivateKeyFile := flag.String("private_key", "", "File containing cient private RSA key")
	clientPublicKeyFile := flag.String("public_key", "", "File containing client public X509 key")
	caPublicKeyFile := flag.String("ca_public_key", "", "Public key of a CA used to sign all public certificates")
	jwtPrivateKey := flag.String("jwt_private_key", "", "Private key to sign JWT token")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	//////////////////////////////////////gRPC client//////////////////////////////////////
	// NOTE: This should be an address instead of a container name
	cert, err := tls.LoadX509KeyPair(*clientPublicKeyFile, *clientPrivateKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to load public/private keys pair %v", err)
	}

	// Create certificate pool from the CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(*caPublicKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to read ca certificate %v", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Logger.Fatal("Failed to append ca certificate %v", err)
	}

	///////////////////////////////////////////////////////////////////////////////
	// JWT authentication
	claims := jwtauth.JWTClaims{
		// NOTE: These should come from environment variables
		Username: "saml",
		Password: "saml",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)), // expires in 24h
		},
	}

	// Parse jwt signing private key
	jwtPrivateKeyContents, err := os.ReadFile(*jwtPrivateKey)
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
	pemBlock, _ := pem.Decode([]byte(jwtPrivateKeyContents))
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

	jwtAuthManager := jwtauth.JWTAuthManager{
		Token: token,
	}

	///////////////////////////////////////////////////////////////////////////////
	// Test jwt token validation
	//	tokenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	//	if err != nil {
	//		log.Logger.Fatal("Failed to create token validator %v", err)
	//	}
	//
	//	validToken, err := tokenValidator.GetToken(token)
	//	if err != nil {
	//		log.Logger.Fatal("Unable to get validated token %v", err)
	//	}
	//
	//	log.Logger.Info("Token header: %v", validToken.Header)
	//	log.Logger.Info("Token claims: %v", validToken.Claims)
	//
	///////////////////////////////////////////////////////////////////////////////
	options := []grpc.DialOption{
		grpc.WithPerRPCCredentials(jwtAuthManager),
		grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				// IMPORTANT: Server name should be equal to Common Name on the certificate
				ServerName:   "localhost",
				Certificates: []tls.Certificate{cert},
				RootCAs:      certPool,
				MinVersion:   tls.VersionTLS13,
			}),
		),
	}

	txnServiceAddr := fmt.Sprintf("transaction-service:%d", *txnPort)
	grpcClient, err := grpc.NewClient(txnServiceAddr, options...)
	if err != nil {
		log.Logger.Fatal("Failed to create grpc client %v", err)
	}
	// NOTE: Should we defer closing the connection?
	defer grpcClient.Close()

	txnClient := api.NewTransactionServiceClient(grpcClient)

	kvsService := kvs.NewService(&settings, txnClient)

	doneChan := make(chan bool, 1)
	osSigChan := make(chan os.Signal, 1)

	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(doneChan)
		err := kvsService.Run()
		if err != nil {
			log.Logger.Error("Service terminated with an error %v", err)
			close(osSigChan)
		} else {
			log.Logger.Info("Service shut down gracefully")
		}
	}()

	<-osSigChan
	kvsService.Close()
	<-doneChan
}
