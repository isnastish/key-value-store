package proto

import (
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate go install google.golang.org/grpc/cmd/protoc-gen-go-grpc
//go:generate -command generate-proto ../external/protoc-25.4/bin/protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative
//go:generate generate-proto api/api.proto
