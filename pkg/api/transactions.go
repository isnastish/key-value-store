package api

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/isnastish/kvs/pkg/txn_service"
	"github.com/isnastish/kvs/proto/api"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

type transactionServer struct {
	api.UnimplementedTransactionServiceServer
	service     *txn_service.TransactionService
	serviceDesc *grpc.ServiceDesc
}

func (s *transactionServer) ServiceDesc() *grpc.ServiceDesc {
	return s.serviceDesc
}

func NewTransactionServer(service *txn_service.TransactionService) *transactionServer {
	return &transactionServer{
		service:     service,
		serviceDesc: &api.TransactionService_ServiceDesc,
	}
}

func (s *transactionServer) ReadTransactions(_ *emptypb.Empty, stream api.TransactionService_ReadTransactionsServer) error {
	log.Logger.Info("ReadWriteTransactions rpc was called")

	transactionChan, errorChan := s.service.ReadTransactions()
	for {
		select {
		case transact := <-transactionChan:
			log.Logger.Info("Read transaction %s", transact.String())

			var transactionData *api.TransactionData

			if transact.TxnType == apitypes.TransactionPut {
				switch transact.StorageType {
				case apitypes.StorageInt:
					transactionData = &api.TransactionData{Kind: &api.TransactionData_IntValue{IntValue: transact.Data.(int32)}}

				case apitypes.StorageUint:
					transactionData = &api.TransactionData{Kind: &api.TransactionData_UintValue{UintValue: transact.Data.(uint32)}}

				case apitypes.StorageFloat:
					transactionData = &api.TransactionData{Kind: &api.TransactionData_FloatValue{FloatValue: transact.Data.(float32)}}

				case apitypes.StorageString:
					transactionData = &api.TransactionData{Kind: &api.TransactionData_StrValue{StrValue: transact.Data.(string)}}

				case apitypes.StorageMap:
					log.Logger.Info("txn: map data: %v", transact.Data.(map[string]string))
					transactionData = &api.TransactionData{
						Kind: &api.TransactionData_MapValue{MapValue: &api.Map{Data: transact.Data.(map[string]string)}},
					}
				}
			} else {
				transactionData = &api.TransactionData{Kind: &api.TransactionData_NullValue{}}
			}

			protoTransaction := &api.Transaction{
				TxnType:     api.TxnType(transact.TxnType),
				StorageType: api.StorageType(transact.StorageType),
				Timestamp:   timestamppb.New(transact.Timestamp),
				Key:         transact.Key,
				Data:        transactionData,
			}

			err := stream.Send(protoTransaction)
			if err != nil {
				log.Logger.Error("Failed to send transaction %v", err)
				return err
			}

		case err := <-errorChan:
			if err != io.EOF && err != nil {
				return err
			}
			log.Logger.Info("Done reading transactions")
			return nil
		}
	}
}

func (s *transactionServer) WriteTransactions(stream api.TransactionService_WriteTransactionsServer) error {
	log.Logger.Info("WriteTransactions rpc was called")

	serviceErrorChan := s.service.HandleTransactions()

	receivedTransactionChan := make(chan *api.Transaction)
	streamErrorChan := make(chan error) // 1?

	go func() {
		defer close(streamErrorChan)
		for {
			transact, err := stream.Recv()
			if err != nil {
				streamErrorChan <- err
				return
			}

			log.Logger.Info("txn: received transaction of type %s", transact.TxnType.String())

			receivedTransactionChan <- transact
		}
	}()

	for {
		select {
		case protoTransaction := <-receivedTransactionChan:
			// Value of the transaction received from the client can be nil,
			// we would have to inspect protoTransaction.Value in order to verify that.
			var apiData interface{}

			if protoTransaction.TxnType == api.TxnType_TxnPut {
				switch protoTransaction.StorageType {
				case api.StorageType_StorageInt:
					apiData = protoTransaction.Data.GetIntValue()

				case api.StorageType_StorageUint:
					apiData = protoTransaction.Data.GetUintValue()

				case api.StorageType_StorageFloat:
					apiData = protoTransaction.Data.GetFloatValue()

				case api.StorageType_StorageStr:
					apiData = protoTransaction.Data.GetStrValue()

				case api.StorageType_StorageMap:
					apiData = protoTransaction.Data.GetMapValue().Data
				}
				// TODO: Consider NullValue as well.
			}

			transaction := &apitypes.Transaction{
				TxnType:     apitypes.TransactionType(protoTransaction.TxnType),
				StorageType: apitypes.TransactionStorageType(protoTransaction.StorageType),
				Timestamp:   protoTransaction.Timestamp.AsTime(),
				Key:         protoTransaction.Key,
				Data:        apiData,
			}

			s.service.WriteTransaction(transaction)

		case err := <-streamErrorChan:
			log.Logger.Error("Failed to receive transaction %v", err)
			return err

		case err := <-serviceErrorChan:
			log.Logger.Error("Failed to process incoming transactions %v", err)
			return err
		}
	}
}
