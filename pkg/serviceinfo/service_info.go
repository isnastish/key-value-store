package info

import "strings"

var serviceName = "kvs"
var version = "v1.0.0"

func ServiceVersion() string {
	return strings.ReplaceAll(version, ".", "-")
}

func ServiceName() string {
	return serviceName
}
