package main

import (
	"flag"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
)

func main() {
	var settings kvs.Settings
	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.StringVar(&settings.CertPemFile, "certpem", "cert.pem", "File containing server certificate")
	flag.StringVar(&settings.KeyPemFile, "keypem", "key.pem", "File containing client certificate")
	flag.StringVar(&settings.TransactionLogFile, "transactionFile", "../../transactions.bin", "Path to transaction log file")
	logLevel := flag.String("loglevel", "DEBUG", "Set log level. Feasible values (DEBUG|INFO|WARN|ERROR|FATAL|PANIC|DISABLED)")
	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	service := kvs.NewService(&settings)
	service.Run()
}
