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
	id   uint64
	kind EventKind
	key  string
	val  interface{}
	time time.Time
}
