package kvs

import (
	"time"
)

type EventType int

const maxEventCount = ^uint64(0)

const (
	_ EventType = iota
	eventAdd
	eventGet
	eventDel
	eventIncr
	eventIncrBy
)

var eventToStr map[EventType]string
var strToEvent map[string]EventType

func init() {
	initEventTables()
}

func initEventTables() {
	eventToStr = make(map[EventType]string)
	eventToStr[eventAdd] = "event_add"
	eventToStr[eventGet] = "event_get"
	eventToStr[eventDel] = "event_del"
	eventToStr[eventIncr] = "event_incr"
	eventToStr[eventIncrBy] = "event_incr_by"

	strToEvent = make(map[string]EventType)
	strToEvent["event_add"] = eventAdd
	strToEvent["event_get"] = eventGet
	strToEvent["event_del"] = eventDel
	strToEvent["event_incr"] = eventIncr
	strToEvent["event_incr_by"] = eventIncrBy
}

type Event struct {
	StorageType StorageType
	Id          uint64
	Type        EventType
	Key         string
	Val         interface{}
	Timestamp   time.Time
}
