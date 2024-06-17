package kvs

import (
	"time"
)

type EventKind int8

const (
	_ EventKind = iota
	EventKindPut
	EventKindGet
	EventKindDelete
)

type Event struct {
	id    uint64
	kind  EventKind
	key   string
	value string
	time  time.Time
}

func eventKind2Str(kind EventKind) string {
	switch kind {
	case EventKindPut:
		return "PutEvent"
	case EventKindGet:
		return "GetEvent"
	case EventKindDelete:
		return "DeleteEvent"
	default:
		// panic/log?
		return "unknown"
	}
}

func str2EventKind(eventStr string) EventKind {
	if eventStr == "PutEvent" {
		return EventKindPut
	}

	if eventStr == "GetEvent" {
		return EventKindGet
	}

	if eventStr == "DeleteEvent" {
		return EventKindDelete
	}

	panic("Unknown event kind")
}
