package txn

import (
	"flag"

	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/protobuf/types/known/emptypb"
)

// ///////////////////////////////////api type aliases/////////////////////////////////////
type APIWriteTransactionStream api.TransactionService_WriteTransactionsServer
type APIReadTransactionStream api.TransactionService_ReadTransactionsServer
type APIProcessErrorStream api.TransactionService_ProcessErrorsServer

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

func main() {
	/////////////////////////////////////Transaction service port/////////////////////////////////////
	txnPort := flag.Uint("port", 5051, "Transaction service listening port")
	flag.Parse()

	_ = txnPort
}
