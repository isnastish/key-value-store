package main

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/golang-jwt/jwt/v5"
	"github.com/isnastish/kvs/pkg/api"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/txn_service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type JWTValidator struct {
	key crypto.PublicKey
}

func NewValidator(publicKeyPath string) (*JWTValidator, error) {
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

func (v *JWTValidator) GetToken(tokenString string) (*jwt.Token, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Figure out whether the token came from somebody we trust.
		// check whether a token uses expected signing method.
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return v.key, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token %v", err)
	}

	return token, nil
}

func validateBasicCredentials(incomingCtx context.Context) error {
	md, ok := metadata.FromIncomingContext(incomingCtx)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing credentials metadata")
	}

	if auth, ok := md["authorization"]; ok {
		if len(auth) > 0 {
			token := strings.TrimPrefix(auth[0], "Basic ")
			if token == base64.StdEncoding.EncodeToString([]byte("saml:saml")) {
				return nil
			}
		}
	}

	return status.Errorf(codes.Unauthenticated, "invalid credentials token")
}

// //////////////////////////////////////////////////////////////////
// Server-side stream interceptor
func readTransactionsStremInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	log.Logger.Info("Invoked server streaming interceptor")
	//	if err := validateBasicCredentials(ss.Context()); err != nil {
	//		log.Logger.Error("Authorization failed %v", err)
	//		return err
	//	}
	//
	log.Logger.Info("Authorization succeeded")

	err := handler(srv, ss)
	if err != nil {
		log.Logger.Info("Failed to invoke streaming RPC with error %v", err)
	}

	return nil
}

func main() {
	postgresUrl := flag.String("postgres_endpoint", "", "Postgres database URL")
	logLevel := flag.String("log_level", "info", "Log level")
	grpcPort := flag.Uint("grpc_port", 5051, "GRPC listening port")
	loggerBackend := flag.String("backend", "postgres", "Backend for logging transactions [file|postgres]")
	serverPrivateKeyFile := flag.String("private_key", "", "Server private RSA key")
	serverPublicKeyFile := flag.String("public_key", "", "Server public X509 key")
	caPublicKeyFile := flag.String("ca_public_key", "", "Public kye of a CA used to sign all public certificates")
	allowUnauthorized := flag.Bool("allow_unauthorized", false, "Disable authorization")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	var transactLogger txn_service.TransactionLogger

	switch *loggerBackend {
	case "postgres":
		postgresLogger, err := txn_service.NewPostgresTransactionLogger(*postgresUrl)
		if err != nil {
			log.Logger.Fatal("Failed to create database transaction logger %v", err)
		}
		transactLogger = postgresLogger

	case "file":
		const filepath = ""
		fileLogger, err := txn_service.NewFileTransactionLogger(filepath)
		if err != nil {
			log.Logger.Fatal("Failed to create file transaction logger %v", err)
		}
		transactLogger = fileLogger
	}

	// parse server's public/private keys
	cert, err := tls.LoadX509KeyPair(*serverPublicKeyFile, *serverPrivateKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to parse public/private key pair %v", err)
	}

	// create a certificate pool from CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(*caPublicKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to rea ca certificate %v", err)
	}

	// append the client certificates from the CA to the certificate pool
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Logger.Fatal("Failed to append ca certificate %v", err)
	}

	options := []grpc.ServerOption{
		grpc.StreamInterceptor(readTransactionsStremInterceptor),

		// grpc.StreamInterceptor(),
		grpc.Creds(
			credentials.NewTLS(&tls.Config{
				// request client certificate during handshake and do the validation
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{cert},
				// root certificate authorities that servers use
				// to verify a client certificate by the policy in ClientAuth
				ClientCAs: certPool,
			}),
		),
	}

	transactionService := txn_service.NewTransactionService(transactLogger, *allowUnauthorized)

	// pass TLS credentials to create  secure grpc server
	grpcServer := api.NewGRPCServer(options, api.NewTransactionServer(transactionService))

	doneChan := make(chan bool, 1)
	osSigChan := make(chan os.Signal, 1)
	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(doneChan)
		err := grpcServer.Serve(*grpcPort)
		if err != nil {
			log.Logger.Fatal("Service terminated abnormally %v", err)
		} else {
			log.Logger.Info("Service shutdown gracefully")
		}
	}()

	<-osSigChan
	grpcServer.Close()
	<-doneChan
}
