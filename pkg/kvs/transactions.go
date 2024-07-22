package kvs

import (
	"context"
)

type TxnType string

const (
	txnPut    TxnType = "put"
	txnGet    TxnType = "get"
	txnDelete TxnType = "delete"
	txnIncr   TxnType = "incr"
	txnIncrBy TxnType = "incrby"
)

type TxnLogger interface {
	WriteTransaction(txnType TxnType, storageType StorageType, key string, value interface{})
	ProcessTransactions(ctx context.Context)
	ReadEvents() (<-chan Event, <-chan error)
	WaitForPendingTransactions()
}
