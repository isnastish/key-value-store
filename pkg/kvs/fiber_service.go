// Implementation of the key-value storage service using fiber.
package kvs

import (
	"fmt"

	"github.com/gofiber/fiber/v3"

	"github.com/isnastish/kvs/pkg/apitypes"
	info "github.com/isnastish/kvs/pkg/serviceinfo"
)

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

func (s *FiberService) Server() error {
	app := fiber.New(fiber.Config{
		ServerHeader: "fiber",
	})

	// testing GET method.
	app.Get(fmt.Sprintf("/%s/%s/uint/put/{key:[0-9A-Za-z_]+}", info.ServiceName(), info.ServiceVersion()), func(ctx fiber.Ctx) error {
		return ctx.SendString("hello world from fiber")
	})

	if err := app.Server().ListenAndServe("0.0.0.0:4000"); err != nil {
		return fmt.Errorf("fiber: failed to listen on port 0.0.0.0:4000")
	}

	return nil
}
