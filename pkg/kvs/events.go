package kvs

import (
	"time"
)

const maxEventCount = ^uint64(0)

type StorageType int
type EventType int

const (
	_ StorageType = iota
	storageTypeInt
	storageTypeFloat
	storageTypeUint
	storageTypeString
	storageTypeMap
)

const (
	_ EventType = iota
	eventTypeAdd
	eventTypeGet
	eventTypeDel
	eventTypeIncr
	eventTypeIncrBy
)

type Event struct {
	StorageType StorageType
	Id          uint64
	Type        EventType
	Key         string
	Val         interface{}
	Timestamp   time.Time
}
