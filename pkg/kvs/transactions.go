package kvs

import (
	"context"
	"os"
	"time"
)

type TransactionLogger interface {
	writePutTransaction(key string, val interface{})
	writeGetTransaction(key string)
	writeDeleteTransaction(key string)

	processEvents(ctx context.Context)
	readSavedEvents() (<-chan Event, <-chan error)
}

type DBTransactionLogger struct {
}

type FileTransactionLogger struct {
	id       uint64
	filePath string
	file     *os.File
	events   chan<- Event
	errors   <-chan error
}

func newFileTransactionsLogger(filePath string) (*FileTransactionLogger, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &FileTransactionLogger{
		filePath: filePath,
		file:     file,
	}, nil
}

func (l *FileTransactionLogger) writePutTransaction(key string, val interface{}) {
	l.events <- Event{id: l.id, kind: EventKindPut, key: key, val: val, time: time.Now()}
}

func (l *FileTransactionLogger) writeGetTransaction(key string) {
	l.events <- Event{id: l.id, kind: EventKindGet, key: key, time: time.Now()}
}

func (l *FileTransactionLogger) writeDeleteTransaction(key string) {
	l.events <- Event{id: l.id, kind: EventKindDelete, key: key, time: time.Now()}
}

func (l *FileTransactionLogger) processEvents(ctx context.Context) {
	events := make(chan Event, 64) // Why the channel is buffered?
	errors := make(chan error, 64)

	// TODO: Figure out why do we assign events buffered channel to l.events
	l.events = events
	l.errors = errors

	for {
		select {
		case <-events:

		case <-ctx.Done():
			for event := range events {
				// write events
				_ = event
			}
			return
		}
	}

	// Event format
	//    {
	//    	id: 0;
	//    	kind: EventKindPut;
	//    	key: "key";
	//    	value: "value" | e;
	//    	time: time;
	//    },
	// TODO: Figure out the format for uint64
	// for event := range events {
	// 	l.id++

	// 	_, err := fmt.Fprintf(l.file,
	// 		"{\n"+
	// 			"\tid: %d;\n"+
	// 			"\tkind: %s;\n"+
	// 			"\tkey: %s;\n"+
	// 			"\tvalue: %s;\n",
	// 		"\ttime: %s;\n"+
	// 			"},\n",
	// 		event.id,
	// 		eventKind2Str(event.kind),
	// 		event.key,
	// 		event.value,
	// 		event.time.Format(time.DateTime),
	// 	)

	// 	if err != nil {
	// 		errors <- err
	// 		return
	// 	}
	// }
}

func (l *FileTransactionLogger) readSavedEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
