package kvs

import (
	"time"
)

type EventType int
type StorageType int

const maxEventCount = ^uint64(0)

const (
	_ StorageType = iota
	storageInt
	storageFloat
	storageString
	storageUint
	storageMap
)

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
	case storageInt:
		return "IntStorage"
	case storageFloat:
		return "FloatStorage"
	case storageString:
		return "StringStorage"
	case storageMap:
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
