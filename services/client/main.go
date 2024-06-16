package main

import (
	"flag"
	"fmt"

	kvs "github.com/isnastish/kvs/pkg/client"
)

func main() {
	var settings kvs.Settings
	flag.StringVar(&settings.KvsEndpoint, "endpoint", "localhost:8080", "KVS service endpoint")
	flag.Parse()

	kvsClient := kvs.Client(&settings)

	res := kvsClient.Hello()
	if res.Error() != nil {
		fmt.Printf("error %v\n", res.Error())
		return
	}
	fmt.Println(res.Value())
}
