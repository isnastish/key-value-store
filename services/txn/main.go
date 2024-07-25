package main

import (
	"context"
	"os"

	// TODO: Rename to api?
	proto "github.com/isnastish/kvs/proto/transactions"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// type TxnLogger interface {
// }

// type TxnFileLogger struct {
// }

// type TxnPostgresLogger struct {
// 	eventsChan chan Event
// 	errorsChan chan error
// 	dbpool     *pgxpool.Pool
// }

type TxnService struct {
	errorsChan chan error
	// eventsChan chan Event
}

func (s *TxnService) WriteIntTxn(context.Context, *proto.IntTxn) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteFloatTxn(context.Context, *proto.FloatTxn) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteStrTxn(context.Context, *proto.StrTxn) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TxnService) WriteMapTxn(context.Context, *proto.MapTxn) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *TxnService) ProcessTxnErrors(_ *emptypb.Empty, stream proto.TransactionService_ProcessTxnErrorsServer) error {
	// These stream should always be open on the client side,
	// we have to poll errors constantly

	for {
		select {
		case err := <-s.errorsChan:
			stream.Send(
				&proto.Error{Message: err.Error()},
			)
			// shutdown the service?
		}
	}

	// end of the stream
	// return nil
}

func (s *TxnService) ReadIntTxns(_ *emptypb.Empty, stream proto.TransactionService_ReadIntTxnsServer) error {
	// Read row from the database, and send it to the client.
	valueReadFromIntTable := 12
	keyReadFromIntTable := "key"

	stream.Send(
		&proto.IntTxn{
			Base: &proto.TxnBase{
				TxnType:   proto.TxnType_TXN_PUT,
				Timestamp: &timestamppb.Timestamp{},
				Key:       keyReadFromIntTable,
			},
			Value: int32(valueReadFromIntTable)},
	)

	// mark the end of all the transactions
	return nil
}

func (s *TxnService) ReadFloatTxns(_ *emptypb.Empty, stream proto.TransactionService_ReadFloatTxnsServer) error {
	return nil
}

func (s *TxnService) ReadStrTxn(_ *emptypb.Empty, stream proto.TransactionService_ReadStrTxnServer) error {
	return nil
}

func (s *TxnService) ReadMapTxn(_ *emptypb.Empty, stream proto.TransactionService_ReadMapTxnServer) error {
	return nil
}

func main() {
	const port = 5051

	server := grpc.NewServer()
	_ = server
	// proto.RegisterTransactionServiceServer(server)

	os.Exit(0)
}
