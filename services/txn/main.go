package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"
)

type TransactionLogger interface {
	WriteTransaction(event *api.Event)
	ProcessIncomingEvents()
}

type PostgresTransationLogger struct {
	EventChan chan *api.Event
	// ErrorChan chan
	ConnPool *pgxpool.Pool
}

func (l *PostgresTransationLogger) WriteTransaction(event *api.Event) {
	l.EventChan <- event
}

func (l *PostgresTransationLogger) ProcessIncomingEvents() {

}

func NewPostgresTransactionLogger() {

}

type TransactionService struct {
	api.UnimplementedTransactionServiceServer
	logger TransactionLogger
}

func (s *TransactionService) ReadTransactions(_ *emptypb.Empty, stream api.TransactionService_ReadTransactionsServer) error {
	log.Logger.Info("ReadTransactions stream was opened")
	for i := 0; i < 10; i++ {
		event := &api.Event{
			TxnType:     api.TxnType_TxnPut,
			StorageType: api.StorageType_StorageInt,
			Key:         "my_key",
			Value:       &api.Event_FloatValue{FloatValue: 3.2340 + float32(i)},
		}
		stream.Send(event)
	}
	return nil
}

func (s *TransactionService) WriteTransactions(stream api.TransactionService_WriteTransactionsServer) error {
	log.Logger.Info("WriteTransactions stream was opened")

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Logger.Info("Write transactions stream was closed")
				break
			}
		}
		s.logger.WriteTransaction(event)
	}

	return nil
}

func (s *TransactionService) ProcessErrors(_ *emptypb.Empty, stream api.TransactionService_ProcessErrorsServer) error {
	log.Logger.Info("ProcessErrors stream was opened")

	// Send a test error
	err := &api.Error{Message: "Failed to save transaction"}
	stream.Send(err)

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
