package kvs

import (
	"context"
	"encoding/gob"
	"io"
	"os"
	"time"

	"github.com/isnastish/kvs/pkg/log"
)

// TODO: Use encoding/decoding for the chat as well to encode/decode messages, not raw bytes
// https://stackoverflow.com/questions/35845596/is-a-struct-actually-copied-between-goroutines-if-sent-over-a-golang-channel
// Package for encoding/decoding data sent over the network (to be explored)
// https://pkg.go.dev/encoding/gob

type FileTransactionLogger struct {
	id       uint64
	filePath string
	file     *os.File
	events   chan<- Event
	errors   <-chan error
	enc      *gob.Encoder
	dec      *gob.Decoder
}

func newFileTransactionsLogger(filePath string) (*FileTransactionLogger, error) {
	// Seek method cannot be used on files created with O_APPEND file,
	// we would have to advance the seek pointer manually
	// Since we always read the file first, readEvents will advance the seek ptr,
	// and then write will happen at the right location, so we can omit os.O_APPEND flag
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}

	return &FileTransactionLogger{
		filePath: filePath,
		file:     file,
		enc:      gob.NewEncoder(file),
		dec:      gob.NewDecoder(file),
	}, nil
}

func (l *FileTransactionLogger) writeEvent(eventType EventType, storageType StorageType, key string, values ...interface{}) {
	// NOTE: Intentionally not specifying id, since it will be accessed by multiple goroutines
	var val interface{}
	if len(values) > 0 {
		val = values[0]
	}
	l.events <- Event{
		Type:        eventType,
		StorageType: storageType,
		Key:         key,
		Val:         val,
		Timestamp:   time.Now()}
}

func (l *FileTransactionLogger) writeEvents(ctx context.Context) {
	defer l.file.Close()

	events := make(chan Event, 32)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors

	encodeEvent := func(event *Event) bool {
		if err := l.enc.Encode(event); err != nil {
			log.Logger.Error("Failed to encode an event %v", err)
			errors <- err
			return false
		}
		return true
	}

	for {
		select {
		case event := <-events:
			// Might be accessed by multiple threads?
			event.Id = l.id
			if !encodeEvent(&event) {
				return
			}
			l.id++

		case <-ctx.Done():
			// If the service terminates for some reason,
			// we want to save all the events, which are left in the buffer.
			// Otherwise they might get lost.
			for event := range events {
				event.Id = l.id
				if !encodeEvent(&event) {
					return
				}
				l.id++
			}
			return
		}
	}
}

func (l *FileTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	events := make(chan Event)
	errors := make(chan error, 1)

	// NOTE: Be careful with io.EOF
	go func() {
		// The receiver still will be able to read from closed channels
		// but not write to them.
		defer close(events)
		defer close(errors)

		// Is it even necessary?
		_, err := l.file.Seek(0, io.SeekStart)
		if err != nil {
			log.Logger.Error("Failed to set the file offset for reading %v", err)
			errors <- err
			return
		}

		for {
			event := Event{}
			err := l.dec.Decode(&event)
			if err != nil && err != io.EOF {
				log.Logger.Error("Error while decoding event %v", err)
				errors <- err
				break
			}
			events <- event
		}
		// set seek value here?
	}()

	return events, errors
}
