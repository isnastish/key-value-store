package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var settings kvs.ServiceSettings
	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.StringVar(&settings.CertPemFile, "certpem", "cert.pem", "File containing server certificate")
	flag.StringVar(&settings.KeyPemFile, "keypem", "key.pem", "File containing client certificate")
	flag.BoolVar(&settings.TxnDisabled, "txn_disabled", false, "Disable transactions. Enabled by default")
	txnFilePath := flag.String("txn_file_name", "transactions.bin", "Path to transaction log file if file transaction logging selected")
	txnLoggerType := flag.String("txn_type", "db", "Transaction logger type. Either log transaction to a database or a file (db|file)")
	logLevel := flag.String("log_level", "debug", "Set global logging level. Feasible values are: (debug|info|warn|error|fatal|panic|disabled)")
	txnPort := flag.Uint("txn-port", 5051, "Transaction service listening port")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	var txnLogger kvs.TxnLogger
	var err error

	switch strings.ToLower(*txnLoggerType) {
	case "file":
		txnLogger, err = kvs.NewFileTxnLogger(*txnFilePath)
		if err != nil {
			log.Logger.Fatal("Failed to init file transaction logger %v", err)
			os.Exit(1)
		}

	case "db":
		// NOTE: For development only.
		// postgres-db is the name of a container running postgresql database on the same network
		// as our kvs service. The container is specified in compose.yaml file
		os.Setenv("DATABASE_URL", "postgresql://postgres:nastish@postgres-db:5432/postgres?sslmode=disable")

		postgresURL := os.Getenv("DATABASE_URL")
		if postgresURL == "" {
			log.Logger.Fatal("Database transaction logging is enabled, but a database URL is not specified")
			os.Exit(1)
		}

		txnLogger, err = kvs.NewDBTxnLogger(postgresURL)
		if err != nil {
			log.Logger.Fatal("Failed to init DB transaction logger %v", err)
			os.Exit(1)
		}

	default:
		log.Logger.Fatal("Unknown transaction logger type %s", *txnLoggerType)
		os.Exit(1)
	}

	settings.TxnLogger = txnLogger

	//////////////////////////////////////gRPC client//////////////////////////////////////
	// NOTE: This should be an address instead of a container name
	txnServiceAddr := fmt.Sprintf("transaction-service:%d", *txnPort)
	grpcClient, err := grpc.NewClient(txnServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Logger.Fatal("Failed to create grpc client %v", err)
		os.Exit(1)
	}
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
