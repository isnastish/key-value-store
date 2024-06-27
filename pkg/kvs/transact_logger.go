package kvs

import (
	"context"
)

// Figure out whether it's possible to endcode struct which have interface{} fields.
// Because if we cannot, we would have to create multiple events, for each storage type.
// But we wouldn't know how to read those events from the file, if they are of different types

type TransactionLogger interface {
	writeEvent(evenType EventType, storageType StorageType, key string, values ...interface{})
	processEvents(ctx context.Context)
	readEvents() (<-chan Event, <-chan error)
}
