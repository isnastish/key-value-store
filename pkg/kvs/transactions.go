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

func (l *FileTransactionLogger) processEvents() {
	events := make(chan Event, 16)
	l.events = events

	for _, event := range events {
		// Write an event to a file
	}
}
