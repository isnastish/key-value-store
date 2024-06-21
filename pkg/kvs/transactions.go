package kvs

import (
	"context"
	"fmt"
	"os"
	"time"
)

type TransactionLogger interface {
	writePutEvent(key string, val interface{})
	writeGetEvent(key string)
	writeDeleteEvent(key string)

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

// Each of these functions might block, if the buffer is full
// Maybe use reflect.Value instead
func (l *FileTransactionLogger) writePutEvent(key string, val interface{}) {
	l.events <- Event{id: l.id, kind: EventKindPut, key: key, val: val, time: time.Now()}
}

func (l *FileTransactionLogger) writeGetEvent(key string) {
	l.events <- Event{id: l.id, kind: EventKindGet, key: key, time: time.Now()}
}

func (l *FileTransactionLogger) writeDeleteEvent(key string) {
	l.events <- Event{id: l.id, kind: EventKindDelete, key: key, time: time.Now()}
}

func _wrap(src string) string {
	return "\t" + src + "\n"
}

func (l *FileTransactionLogger) processEvents(ctx context.Context) {
	defer l.file.Close()

	// TODO: The data should be written in a binary format,
	// instead of plain text. Instead of writing each field of an Event
	// struct, we could write the whole struct itself
	events := make(chan Event, 32)
	errors := make(chan error, 32)

	l.events = events
	l.errors = errors

	for {
		select {
		case event := <-events:
			if l.id == MaxEventId {
				panic("Exceeded maximum event capacity")
			}

			l.id++

			_, err := fmt.Fprintf(
				l.file, "Event::{\n"+
					_wrap("Id:%d;")+
					_wrap("Kind:%s;")+
					_wrap("Key:%s;")+
					_wrap("Val:%v;")+
					_wrap("Time:%s")+
					"};\n", event.id, event.kind.toStr(), event.key, event.val, event.time.Format(time.DateTime),
			)

			if err != nil {
				errors <- err
				return // return ?
			}

		case <-ctx.Done():
			// exhaust the channel if it was full and consume the remaining events
			for event := range events {
				_ = event
			}
			return
		}
	}
}

func (l *FileTransactionLogger) readSavedEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
