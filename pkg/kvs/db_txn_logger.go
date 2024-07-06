package kvs

import "context"

type DBTransactionLogger struct {
}

func newDBTransactionLogger() (*DBTransactionLogger, error) {
	return nil, nil
}

func (l *DBTransactionLogger) writeTransaction(evenType EventType, storageType StorageType, key string, values ...interface{}) {

}

func (l *DBTransactionLogger) processTransactions(ctx context.Context) {

}

func (l *DBTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
