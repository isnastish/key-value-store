package main

import (
	"flag"
	"os"
	"strings"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
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
		// For development
		os.Setenv("DATABASE_URL", "postgresql://postgres:nastish@127.0.0.1:5432/postgres?sslmode=disable")

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

	service := kvs.NewService(&settings)
	service.Run()
}
