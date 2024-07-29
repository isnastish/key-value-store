package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	proto "github.com/isnastish/kvs/proto/transactions"
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

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	var txnLogger kvs.TxnLogger
	var err error

	switch strings.ToLower(*txnLoggerType) {
	case "file":
		txnLogger, err = kvs.NewFileTxnLogger(*txnFilePath)
		if err != nil {
			log.Logger.Fatal("Failed to init file transaction logger %v", err)
		}

	case "db":
		// NOTE: For development only.
		// postgres-db is the name of a container running postgresql database on the same network
		// as our kvs service. The container is specified in compose.yaml file

		// NOTE: For debugging (make sure to run postgreSQL inside the docker container first)
		os.Setenv("DATABASE_URL", "postgresql://postgres:nastish@localhost:4040/postgres?sslmode=disable")

		// NOTE: For production
		// os.Setenv("DATABASE_URL", "postgresql://postgres:nastish@postgres-db:5432/postgres?sslmode=disable")

		postgresURL := os.Getenv("DATABASE_URL")
		if postgresURL == "" {
			log.Logger.Fatal("Database transaction logging is enabled, but a database URL is not specified")
		}

		txnLogger, err = kvs.NewDBTxnLogger(postgresURL)
		if err != nil {
			log.Logger.Fatal("Failed to init DB transaction logger %v", err)
		}

	default:
		log.Logger.Fatal("Unknown transaction logger type %s", *txnLoggerType)
	}

	settings.TxnLogger = txnLogger

	conn, err := grpc.NewClient(":5051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Logger.Fatal("Failed to connect to txn service %v", err)
	}

	go func() {
		// TODO: Add a function for gracefully shutting down the service,
		// and remove kill endpoint.
		service := kvs.NewService(&settings, proto.NewTransactionServiceClient(conn))
		service.Run()
	}()

	osSignalChan := make(chan os.Signal, 1)
	signal.Notify(osSignalChan, syscall.SIGINT, syscall.SIGTERM)
	<-osSignalChan
	// service.Close()

	os.Exit(0)
}
