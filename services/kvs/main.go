package main

import (
	"flag"

	"github.com/isnastish/kvs/pkg/kvs"
)

func main() {
	var settings kvs.Settings
	flag.StringVar(&settings.Addr, "address", "localhost:8080", "Address to run a key-value storage on")
	flag.Parse()

	kvs.RunServer(&settings)
}
