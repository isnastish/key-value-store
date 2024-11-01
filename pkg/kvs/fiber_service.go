// NOTE: This is an experimental module using fiber
// Once the implementation is tested properly, we could do the migration
package kvs

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
	info "github.com/isnastish/kvs/pkg/serviceinfo"
)

type FiberService struct {
	storage map[apitypes.TransactionStorageType]Storage
	app     *fiber.App
	running bool
}

func NewFiberService() *FiberService {
	service := &FiberService{}

	// Init storage
	service.storage[apitypes.StorageInt] = newIntStorage()
	service.storage[apitypes.StorageUint] = newUintStorage()
	service.storage[apitypes.StorageFloat] = newFloatStorage()
	service.storage[apitypes.StorageString] = newStrStorage()
	service.storage[apitypes.StorageMap] = newMapStorage()

	// TODO: Pass config as a command-line argument
	service.app = fiber.New(
		fiber.Config{
			ServerHeader: "fiber",
		},
	)

	return service
}

func (s *FiberService) Serve() error {
	route := fmt.Sprintf("/%s/%s/uint/{key:[0-9A-Za-z_]+}", info.ServiceName(), info.ServiceVersion())

	log.Logger.Info("Running fiber service")

	s.app.Get(route, func(ctx *fiber.Ctx) error {
		return ctx.SendString("<h1>Hello world from Fiber</h1>")
	})

	s.running = true

	if err := s.app.Server().ListenAndServe("0.0.0.0:4000"); err != nil {
		return fmt.Errorf("fiber: failed to listen on port 0.0.0.0:4000")
	}

	return nil
}

func (s *FiberService) Close() error {
	if s.running {
		return s.app.Shutdown()
	}

	log.Logger.Warn("Fiber server is not running")

	return nil
}
