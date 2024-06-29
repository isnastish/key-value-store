package kvs

import "context"

type DBTransactionLogger struct {
}

func (l *DBTransactionLogger) writeEvent(evenType EventType, storageType StorageType, key string, val interface{}) {

}

func (l *DBTransactionLogger) processEvents(ctx context.Context) {

}

func (l *DBTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
