package main

import (
	"flag"

	"github.com/isnastish/kvs/pkg/kvs"
)

func main() {
	var settings kvs.Settings
	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.StringVar(&settings.CertPemFile, "certpem", "cert.pem", "File containing server certificate")
	flag.StringVar(&settings.KeyPemFile, "keypem", "key.pem", "File containing client certificate")
	flag.Parse()

	kvs.RunServer(&settings)
}
