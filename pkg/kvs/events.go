package kvs

import (
	"time"
)

const maxEventCount = ^uint64(0)

type EventType string

// NOTE: This might be slow when we have a lot of events,
// thus the same amount of string comparison has to be done. So, replacing it with integers,
// would definitely speed up the process, but we would still have to make lookups
// every time we scan throw rows in a database containing events.
const (
	eventPut    EventType = "put"
	eventGet    EventType = "get"
	eventDel    EventType = "delete"
	eventIncr   EventType = "incr"
	eventIncrBy EventType = "incrby"
)

type Event struct {
	storageType StorageType
	id          uint64
	t           EventType
	key         string
	value       interface{}
	timestamp   time.Time
}
