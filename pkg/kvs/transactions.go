package kvs

import (
	"context"
)

// Figure out whether it's possible to endcode struct which have interface{} fields.
// Because if we cannot, we would have to create multiple events, for each storage type.
// But we wouldn't know how to read those events from the file, if they are of different types

type TxnLoggerType int8

const (
	_ TxnLoggerType = iota
	TxnLoggerTypeDB
	TxnLoggerTypeFile
)

type TansactionLogger interface {
	WriteTransaction(evenType EventType, storageType StorageType, key string, value interface{})
	ProcessTransactions(ctx context.Context)
	ReadEvents() (<-chan Event, <-chan error)
	WaitForPendingTransactions()
}
