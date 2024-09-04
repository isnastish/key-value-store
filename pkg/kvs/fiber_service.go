// Implementation of the key-value storage service using fiber.
package kvs

import "github.com/isnastish/kvs/pkg/apitypes"

type FiberService struct {
	storage map[apitypes.TransactionStorageType]Storage
}

func NewFiberService() *FiberService {
	service := &FiberService{}

	// Init storage
	service.storage[apitypes.StorageInt] = newIntStorage()
	service.storage[apitypes.StorageUint] = newUintStorage()
	service.storage[apitypes.StorageFloat] = newFloatStorage()
	service.storage[apitypes.StorageString] = newStrStorage()
	service.storage[apitypes.StorageMap] = newMapStorage()

	return service
}
