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

func (e EventType) toStr() string {
	switch e {
	case eventAdd:
		return "EventAdd"
	case eventGet:
		return "EventGet"
	case eventDel:
		return "EventDel"
	case eventIncr:
		return "EventIncr"
	case eventIncrBy:
		return "EventIncrBy"
	}
	return "UnknownEvent"
}

func (e StorageType) toStr() string {
	switch e {
	case storageTypeInt:
		return "IntStorage"
	case storageTypeFloat:
		return "FloatStorage"
	case storageTypeString:
		return "StringStorage"
	case storageTypeMap:
		return "MapStorage"
	}
	return "UnknownStorage"
}

type Event struct {
	StorageType StorageType
	Id          uint64
	Type        EventType
	Key         string
	Val         interface{}
	Timestamp   time.Time
}
