package version

import "strings"

var version = "v1.0.0"

func GetServiceVersion() string {
	return strings.ReplaceAll(version, ".", "-")
}
