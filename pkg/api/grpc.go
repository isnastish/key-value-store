package api

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/isnastish/kvs/pkg/log"
)

type GRPCServer struct {
	server *grpc.Server
}

type GrpcService interface {
	ServiceDesc() *grpc.ServiceDesc
}

func NewGRPCServer(service GrpcService, opt ...grpc.ServerOption) *GRPCServer {
	server := &GRPCServer{
		server: grpc.NewServer(opt...),
	}

	// Could be put into a for loop for multiple services
	desc := service.ServiceDesc()
	server.server.RegisterService(desc, service)

	return server
}

func (s *GRPCServer) Serve(port uint) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Logger.Error("grpc: failed to listen %v", err)
		return fmt.Errorf("failed to listen %v", err)
	}

	log.Logger.Info("Listening on port 0.0.0.0:%d", port)

	err = s.server.Serve(listener)
	if err != nil {
		log.Logger.Error("Failed to serve %v", err)
	}

	return err
}

func (s *GRPCServer) Close() {
	s.server.Stop()
}
