package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

//api type aliases/////////////////////////////////////

type APIWriteTransactionStream api.TransactionService_WriteTransactionsServer
type APIReadTransactionStream api.TransactionService_ReadTransactionsServer
type APIProcessErrorStream api.TransactionService_ProcessErrorsServer

/////////////////////////////////////transaction service implementation/////////////////////////////////////

type TransactionService struct {
	api.UnimplementedTransactionServiceServer
}

func (s *TransactionService) ReadTransactions(_ *emptypb.Empty, stream APIReadTransactionStream) error {
	return nil
}

func (s *TransactionService) WriteTransactions(stream APIWriteTransactionStream) error {
	return nil
}

func (s *TransactionService) ProcessErrors(_ *emptypb.Empty, stream APIProcessErrorStream) error {
	return nil
}

//////////////////////////////////grpc server wrapper//////////////////////////////////

type GRPCServer struct {
	server *grpc.Server
}

func NewGRPCServer() *GRPCServer {
	return &GRPCServer{
		server: grpc.NewServer(),
	}
}

func (s *GRPCServer) Serve(port uint) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Logger.Error("grpc: failed to listen %v", err)
		return fmt.Errorf("failed to listen %v", err)
	}

	log.Logger.Info("Listening on port 0.0.0.0:%d", port)

	// Will return a non-nil error unless Stop is called
	err = s.server.Serve(listener)
	if err != nil {
		log.Logger.Error("Failed to serve %v", err)
	}

	return err
}

func (s *GRPCServer) Close() {
	s.server.Stop()
}

func main() {
	/////////////////////////////////////Transaction service port/////////////////////////////////////
	txnPort := flag.Uint("port", 5051, "Transaction service listening port")
	flag.Parse()

	grpcServer := NewGRPCServer()

	doneChan := make(chan bool, 1)

	go func() {
		defer close(doneChan)
		// TODO: Handle errors
		grpcServer.Serve(*txnPort)
	}()

	osSigChan := make(chan os.Signal, 1)
	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	<-osSigChan
	grpcServer.Close()
	<-doneChan
}
