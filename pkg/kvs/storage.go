package kvs

import (
	"sync"

	"github.com/pingcap/errors"
)

type StorageType int

const (
	_ StorageType = iota
	storageTypeInt
	storageTypeFloat
	storageTypeString
	storageTypeUint
	storageTypeMap
)

type Storage interface {
	Add(key string, cmd *CmdResult) *CmdResult
	Get(key string, cmd *CmdResult) *CmdResult
	Del(key string, CmdResult *CmdResult) *CmdResult
}

type baseStorage struct {
	// All common members should be added here,
	// to avoid code duplications
	*sync.RWMutex
}

type IntStorage struct {
	baseStorage
	data map[string]int
}

type UintStorage struct {
	baseStorage
	data map[string]uint
}

type FloatStorage struct {
	baseStorage
	data map[string]float32
}

type StrStorage struct {
	baseStorage
	data map[string]string
}

type MapStorage struct {
	baseStorage
	data map[string]map[string]string
}

type CmdResult struct {
	storageType StorageType
	args        []interface{}
	result      interface{}
	deleted     bool
	err         error
}

func newCmdResult(args ...interface{}) *CmdResult {
	return &CmdResult{
		args: args,
	}
}

func newMapStorage() *MapStorage {
	return &MapStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]map[string]string),
	}
}

func newIntStorage() *IntStorage {
	return &IntStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]int),
	}
}

func newUintStorage() *UintStorage {
	return &UintStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]uint),
	}
}

func newFloatStorage() *FloatStorage {
	return &FloatStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]float32),
	}
}

func newStrStorage() *StrStorage {
	return &StrStorage{
		baseStorage: baseStorage{RWMutex: new(sync.RWMutex)},
		data:        make(map[string]string),
	}
}

func (s *MapStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(map[string]string)
	s.Unlock()
	cmd.storageType = storageTypeMap
	return cmd
}

func (s *MapStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = storageTypeMap
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *MapStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = storageTypeMap
	// If the value existed before deletion, cmd.deleted is set to true,
	// false otherwise, so that on the client side we can determine whether the operation modified the storage or not.
	cmd.deleted = exists
	return cmd
}

func (s *StrStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(string)
	s.Unlock()
	cmd.storageType = storageTypeString
	return cmd
}

func (s *StrStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = storageTypeString
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *StrStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = storageTypeString
	cmd.deleted = exists
	return cmd
}

func (s *IntStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(int)
	s.Unlock()
	cmd.storageType = storageTypeInt
	return cmd
}

func (s *IntStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.storageType = storageTypeInt
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *IntStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = storageTypeInt
	cmd.deleted = exists
	return cmd
}

func (s *IntStorage) Incr(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	defer s.Unlock()
	cmd.storageType = storageTypeInt
	val, exists := s.data[hashKey]
	// If the value doesn't exist, we should created a new entry with the specified key,
	// and set the value to zero
	if !exists {
		s.data[hashKey] = 1
		cmd.result = 0
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + 1)
	return cmd
}

func (s *IntStorage) IncrBy(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	defer s.Unlock()
	cmd.storageType = storageTypeInt
	val, exists := s.data[hashKey]
	// The same applies to IncrBy, if the hashKey doesn't exist,
	// we should create a new one and set the value to cmd.val,
	// but return the result as zero
	if !exists {
		s.data[hashKey] = cmd.args[0].(int)
		cmd.result = 0
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + cmd.args[0].(int))
	return cmd
}

func (s *FloatStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	// Consider the precision in the future. Maybe we have to suply the precision
	// together with the value itself
	s.data[hashKey] = float32(cmd.args[0].(float64))
	s.Unlock()
	cmd.storageType = storageTypeFloat
	return cmd
}

func (s *FloatStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	val, exists := s.data[hashKey]
	s.Unlock()
	cmd.storageType = storageTypeFloat
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *FloatStorage) Del(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.storageType = storageTypeFloat
	cmd.deleted = exists
	return cmd
}
