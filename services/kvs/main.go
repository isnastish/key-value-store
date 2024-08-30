package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/golang-jwt/jwt/v5"
	_ "github.com/google/uuid"
)

// NOTE: Experimental basic authentication

const authHeaderPrefix = "Bearer "

type JWTClaims struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	jwt.RegisteredClaims
}

type basicAuth struct {
	username string
	password string
}

func (b basicAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	log.Logger.Info("Encoding username and password using base64 algorithm")

	auth := b.username + ":" + b.password
	enc := base64.StdEncoding.EncodeToString([]byte(auth))
	return map[string]string{
		"authorization": "Basic " + enc,
	}, nil
}

func (b basicAuth) RequireTransportSecurity() bool {
	return true
}

func main() {
	var settings kvs.ServiceSettings

	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.BoolVar(&settings.TransactionsDisabled, "txn_disabled", false, "Disable transactions. Enabled by default")
	logLevel := flag.String("log_level", "info", "Log level")
	txnPort := flag.Uint("txn_port", 5051, "Transaction service listening port")
	clientPrivateKeyFile := flag.String("private_key", "", "File containing cient private RSA key")
	clientPublicKeyFile := flag.String("public_key", "", "File containing client public X509 key")
	caPublicKeyFile := flag.String("ca_public_key", "", "Public key of a CA used to sign all public certificates")
	jwtSigningKey := flag.String("jwt_signing_key", "", "Private key to sign JWT token")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	//////////////////////////////////////gRPC client//////////////////////////////////////
	// NOTE: This should be an address instead of a container name
	cert, err := tls.LoadX509KeyPair(*clientPublicKeyFile, *clientPrivateKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to load public/private keys pair %v", err)
		os.Exit(1)
	}

	// Create certificate pool from the CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(*caPublicKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to read ca certificate %v", err)
		os.Exit(1)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Logger.Fatal("Failed to append ca certificate %v", err)
		os.Exit(1)
	}

	///////////////////////////////////////////////////////////////////////////////
	// JWT authentication
	claims := JWTClaims{
		"saml",
		"saml",

		jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)), // expires in 24h
		},
	}

	_ = claims
	_ = jwtSigningKey

	///////////////////////////////////////////////////////////////////////////////
	// NOTE: Just for testing basic authentication
	auth := basicAuth{
		username: "saml",
		password: "saml",
	}

	options := []grpc.DialOption{
		// authenticate on each grpc call,
		grpc.WithPerRPCCredentials(auth),
		grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				// NOTE: Server name should be equal to Common Name on the certificate
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
		os.Exit(1)
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
