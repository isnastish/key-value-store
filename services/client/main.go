package main

import (
	kvs "github.com/isnastish/kvs/pkg/client"
)

func main() {
	kvsClient := kvs.NewClient(&kvs.Settings{
		Addr: "localhost:8080",
	})

	_ = kvsClient
	// kvsClient.Put()
}
