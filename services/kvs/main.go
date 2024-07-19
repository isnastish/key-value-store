package main

import (
	"flag"
	"strings"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
)

func main() {
	var settings kvs.ServiceSettings
	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.StringVar(&settings.CertPemFile, "certpem", "cert.pem", "File containing server certificate")
	flag.StringVar(&settings.KeyPemFile, "keypem", "key.pem", "File containing client certificate")
	flag.StringVar(&settings.TxnFilePath, "transaction_file", "transactions.bin", "Path to transaction log file if file transaction logging selected")
	flag.BoolVar(&settings.TransactionsDisabled, "disable_transactions", false, "Disable transaction logging (by default disabled)")
	txnLoggerType := flag.String("transaction_logger_type", "file", "Transaction logger type. Either log transaction to a database or a file (db|file)")
	logLevel := flag.String("log_level", "DEBUG", "Sets global logging level. Feasible values are: (DEBUG|INFO|WARN|ERROR|FATAL|PANIC|DISABLED)")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	switch strings.ToLower(*txnLoggerType) {
	case "file":
		settings.TxnLoggerType = kvs.TxnLoggerTypeFile
	case "db":
		settings.TxnLoggerType = kvs.TxnLoggerTypeDB
	default:
		log.Logger.Fatal("Invalid TransactionLogger type %s", *txnLoggerType)
	}

	// NOTE: Use postgres database for transaction logging for now
	settings.TxnLoggerType = kvs.TxnLoggerTypeDB

	service := kvs.NewService(&settings)
	service.Run()
}
