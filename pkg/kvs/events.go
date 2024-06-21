package kvs

import (
	"time"
)

type EventKind int8

const MaxEventId = ^uint64(0)

const (
	_ EventKind = iota
	EventKindPut
	EventKindGet
	EventKindDelete
)

func (e EventKind) toStr() string {
	switch e {
	case EventKindPut:
		return "EventPut"
	case EventKindGet:
		return "EventGet"
	case EventKindDelete:
		return "EventDelete"
	default:
		// We don't know how to handle unknown events
		panic("Unknown event")
	}
}

type Event struct {
	id   uint64
	kind EventKind
	key  string
	val  interface{}
	time time.Time
}
