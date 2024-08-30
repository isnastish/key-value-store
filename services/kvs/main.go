package main

import (
	"crypto/tls"
	"crypto/x509"
	_ "encoding/base64"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	jwtauth "github.com/isnastish/kvs/pkg/jwt_auth"
	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	jwtAuthManager, err := jwtauth.NewJWTAuthManager(*jwtPrivateKey)
	if err != nil {
		log.Logger.Fatal("Failed to create jwt authentication manager %v", err)
	}

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
