package main

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
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

const authBearerPrefix = "Bearer "

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

func readTransactionsStremInterceptor(srv interface{}, ss grpc.ServerStream,
	info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	// NOTE: We should be able to use fmt.Errorf instead of status package,
	// What would be the difference?
	metadata, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing credentials metadata")
	}

	auth, ok := metadata["authorization"]
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing authorization header")
	}

	if len(auth) == 0 {
		return status.Errorf(codes.InvalidArgument, "authorization header is empty")
	}

	// NOTE: The problem here is that we parse the file which contains a public key every time an RPC is invoked,
	// but instead we should do it only once.
	tokenValidator, err := NewTokenValidator("../../certs/jwt_public.pem")
	if err != nil {
		return status.Errorf(codes.Unauthenticated, fmt.Sprintf("failed to create token validator %v", err))
	}

	// NOTE: We don't need the token, instead we could rename GetToken to ValidateToken
	token, err := tokenValidator.GetToken(strings.Trim(auth[0], authBearerPrefix))
	if err != nil {
		return status.Errorf(codes.Unauthenticated, fmt.Sprintf("failed to validate token %v", err))
	}

	_ = token

	err = handler(srv, ss)
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
