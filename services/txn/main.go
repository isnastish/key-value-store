package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	// TODO: Rename to api?
	proto "github.com/isnastish/kvs/proto/transactions"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/isnastish/kvs/pkg/log"

	_ "github.com/isnastish/kvs/pkg/txn"
)

// NOTE: We have to make a trade-off, either we have a separate rpc for each storage type,
// or we maintain an enum StorageType and pass it on the client side.
// Let's try the first approach and see where it lands us.

type TxnLogger interface {
	WriteIntTxn(txnType proto.TxnType, key string, value int64)
	WriteFloatTxn(txnType proto.TxnType, key string, value float64)
	WriteStrTxn(txnType proto.TxnType, key string, value string)
	WriteMapTxn(txnType proto.TxnType, key string, value map[string]string)

	ProcessTxns(ctx context.Context)

	// ReadEvents() (<-chan Event, <-chan error)

	Close()
	// NOTE: This function is yet to be implemented.
	// The idea behind it is to wait for all pending transactions to complete before closing the transaction logger.
	// WaitForPendingTransactions()
}

type TxnPostgresLogger struct {
	// eventsChan chan Event
	errorsChan chan error
	dbpool     *pgxpool.Pool
}

type TransactionService struct {
	proto.UnimplementedTransactionServiceServer
	errorsChan  chan error
	serviceDesc *grpc.ServiceDesc
	logger      TxnLogger
}

func NewTransactionService() *TransactionService {
	return &TransactionService{
		errorsChan: make(chan error),
	}
}

func (s *TransactionService) WriteTransaction(context.Context, *proto.Transaction) (*emptypb.Empty, error) {

}

func (s *TransactionService) ProcessErrors(_ *emptypb.Empty, stream proto.TransactionService_ProcessErrorsServer) error {
	return nil
}

func (s *TransactionService) ReadTransactions(*emptypb.Empty, grpc.ServerStreamingServer[Transaction]) error {

}

func (s *TxnService) WriteIntTxn(ctx context.Context, txn *proto.IntTxn) (*emptypb.Empty, error) {
	log.Logger.Info("Write int txn RPC was called, txnType: %s, key: %s, value: %d",
		txn.Base.TxnType.String(), txn.Base.Key, txn.Value,
	)
	s.logger.WriteIntTxn()

	// insert transaction into the int table
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteFloatTxn(ctx context.Context, txn *proto.FloatTxn) (*emptypb.Empty, error) {
	log.Logger.Info("Write float txn RPC was called, txnType: %s, key: %s, value: %f",
		txn.Base.TxnType.String(), txn.Base.Key, txn.Value,
	)
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteStrTxn(ctx context.Context, txn *proto.StrTxn) (*emptypb.Empty, error) {
	log.Logger.Info("Write str txn RPC was called, txnType: %s, key: %s, value: %s",
		txn.Base.TxnType.String(), txn.Base.Key, txn.Value,
	)
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteMapTxn(ctx context.Context, txn *proto.MapTxn) (*emptypb.Empty, error) {
	log.Logger.Info("Write map txn RPC was called, txnType: %s, key: %s, value: %v",
		txn.Base.TxnType.String(), txn.Base.Key, txn.Value,
	)
	return &emptypb.Empty{}, nil
}

func (s *TxnService) ProcessTxnErrors(_ *emptypb.Empty, errorStream proto.TransactionService_ProcessTxnErrorsServer) error {
	// These stream should always be open on the client side,
	// we have to poll errors constantly
	log.Logger.Info("Process txn errors RPC was called")

	errorStream.Send(
		&proto.Error{Message: "Hello from TXN service"},
	)

	// stream closed
	return nil

	// for {
	// 	select {
	// 	case err := <-s.errorsChan:
	// 		stream.Send(
	// 			&proto.Error{Message: err.Error()},
	// 		)
	// 		// shutdown the service?
	// 	}
	// }
	// end of the stream
	// return nil
}

func (s *TxnService) ReadIntTxns(_ *emptypb.Empty, stream proto.TransactionService_ReadIntTxnsServer) error {
	log.Logger.Info("Read int txns RPC was called.")

	stream.Send(
		&proto.IntTxn{
			Base: &proto.TxnBase{
				TxnType:   proto.TxnType_TXN_PUT,
				Timestamp: &timestamppb.Timestamp{},
				Key:       "some_key",
			},
			Value: 1234,
		},
	)

	stream.Send(
		&proto.IntTxn{
			Base: &proto.TxnBase{
				TxnType:   proto.TxnType_TXN_PUT,
				Timestamp: &timestamppb.Timestamp{},
				Key:       "another_key",
			},
			Value: -23498,
		},
	)

	// mark the end of all the transactions
	return nil
}

func (s *TxnService) ReadFloatTxns(_ *emptypb.Empty, stream proto.TransactionService_ReadFloatTxnsServer) error {
	log.Logger.Info("Read float txns RPC was called")

	return nil
}

func (s *TxnService) ReadStrTxn(_ *emptypb.Empty, stream proto.TransactionService_ReadStrTxnServer) error {
	log.Logger.Info("Read str txns RPC was called")
	return nil
}

func (s *TxnService) ReadMapTxn(_ *emptypb.Empty, stream proto.TransactionService_ReadMapTxnServer) error {
	log.Logger.Info("Read map txns RPC was called")
	return nil
}

type GrpcServer struct {
	server *grpc.Server
}

// type GrpcService interface {
// 	ServiceDesc() *grpc.ServiceDesc
// }

func (s *GrpcServer) Serve(port uint) error {
	addr := fmt.Sprintf("0.0.0.0:%v", port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Logger.Error("Failed to create a listener %v", err)
		return fmt.Errorf("failed to listen %v", err)
	}

	log.Logger.Info("GRPC server is listening on port %s", addr)

	err = s.server.Serve(listener)
	if err != nil {
		log.Logger.Fatal("GRPC failed to serve %v", err)
	}

	return err
}

func (s *GrpcServer) Close() {
	s.server.Stop()
}

func NewGrpcServer() *GrpcServer {
	server := &GrpcServer{
		// TODO: Figure out how to use unary interceptors.
		// Logging would be a good example
		server: grpc.NewServer(),
	}

	// server.server.RegisterService()
	// see: https://stackoverflow.com/questions/20778771/what-is-the-difference-between-0-0-0-0-127-0-0-1-and-localhost
	// NOTE: address: 0.0.0.0 accepts connections on all possible interfaces
	// Register the service here.

	return server
}

func main() {
	grpcPort := flag.Uint("grpc_port", 5051, "GRPC server listening port")
	flag.Parse()

	grpcServer := NewGrpcServer()

	txnService := NewTxnService()
	proto.RegisterTransactionServiceServer(grpcServer.server, txnService)

	go func() {
		grpcServer.Serve(*grpcPort)
	}()

	osSignalChan := make(chan os.Signal, 1)
	signal.Notify(osSignalChan, syscall.SIGINT /*handle os interrupt signal*/, syscall.SIGTERM)
	<-osSignalChan

	// gracefully shutdown grpc serv
	grpcServer.Close()

	os.Exit(0)
}
