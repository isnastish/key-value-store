// TODO: Write data in a binary format
package kvs

import (
	"os"
	"time"
)

type TransactionLogger interface {
	writePutTransaction()
	writeGetTransaction()
	writeDeleteTransaction()

	// Add a function which reads transactions
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

func (l *FileTransactionLogger) writePutTransaction(key string, value string) {
	l.events <- Event{id: l.id, kind: EventKindPut, key: key, value: value, time: time.Now()}
}

func (l *FileTransactionLogger) writeGetTransaction(key string) {
	l.events <- Event{id: l.id, kind: EventKindGet, key: key, time: time.Now()}
}

func (l *FileTransactionLogger) writeDeleteTransaction(key string) {
	l.events <- Event{id: l.id, kind: EventKindDelete, key: key, time: time.Now()}
}

func (l *FileTransactionLogger) writeEvents() {
	events := make(chan Event, 64) // Why the channel is buffered?
	errors := make(chan error, 64)

	l.events = events
	l.errors = errors

	go func() {
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
	}()
}

// func (l *FileTransactionLogger) readEvents() (<-chan Event, <-chan error) {

// }
