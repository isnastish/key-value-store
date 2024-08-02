package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"
)

type TransactionService struct {
	api.UnimplementedTransactionServiceServer
}

func (s *TransactionService) ReadTransactions(_ *emptypb.Empty, stream api.TransactionService_ReadTransactionsServer) error {
	log.Logger.Info("ReadTransactions rpc")
	return nil
}

func main() {
	port := flag.Uint("port", 5051, "Server listening port")
	flag.Parse()

	grpcServer := grpc.NewServer()
	// Register transaction service to grpc server
	api.RegisterTransactionServiceServer(grpcServer, &TransactionService{})

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", *port))
	if err != nil {
		log.Logger.Fatal("Failed to create listener %v", err)
	}

	log.Logger.Info("grpc: Listening on port 0.0.0.0:%v", *port)

	doneChan := make(chan bool, 1)
	go func() {
		defer close(doneChan)

		err = grpcServer.Serve(listener)
		if err != nil {
			log.Logger.Fatal("Serve terminated with error %v", err)
		} else {
			log.Logger.Info("grpc: Gracefully shutdown")
		}
	}()

	osSigChan := make(chan os.Signal, 1)
	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)
	<-osSigChan
	grpcServer.Stop()
	<-doneChan
}
