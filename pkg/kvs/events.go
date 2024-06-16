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

/*
{
	id: 0;
	kind: EventKindPut;
	key: "key";
	value: "value" | e;
	time: time;
},
{

}
*/
