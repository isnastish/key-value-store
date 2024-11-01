// NOTE: This is an experimental module using fiber
// Once the implementation is tested properly, we could do the migration
package kvs

import (
	"fmt"

	"github.com/gofiber/fiber/v2"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

type FiberService struct {
	storage map[apitypes.TransactionStorageType]Storage
	app     *fiber.App
	running bool
}

func NewFiberService() *FiberService {
	service := &FiberService{
		storage: make(map[apitypes.TransactionStorageType]Storage),
	}

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
			// NOTE: Setting this to true causes fiber
			// server to fail running dockerized
			// Prefork:      true,
		},
	)

	return service
}

func (s *FiberService) Serve() error {

	// add a couple of testing routes
	s.app.Get("/about", func(ctx *fiber.Ctx) error {
		log.Logger.Info("route: /about")

		aboutHTMLPage := `
		<div>
			<h1>This is my about page</h1>
		</div>
		`

		return ctx.SendString(aboutHTMLPage)
	})

	s.app.Get("/simple/route", func(ctx *fiber.Ctx) error {
		log.Logger.Info("Enpoint was triggered")
		return ctx.SendString("<h1>Hello world from Fiber</h1>")
	})

	s.running = true

	log.Logger.Info("Fiber service is running on port: %s", "0.0.0.0:8080")

	// This has to be executed in a separate goroutine
	if err := s.app.Listen(":8080"); err != nil {
		return fmt.Errorf("fiber: failed to listen on port :8080 %v", err)
	}

	// if err := s.app.Server().ListenAndServe("127.0.0.1:8080"); err != nil {
	// 	return fmt.Errorf("fiber: failed to listen on port 0.0.0.0:4000")
	// }

	return nil
}

func (s *FiberService) Close() error {
	if s.running {
		return s.app.Shutdown()
	}

	log.Logger.Warn("Fiber server is not running")

	return nil
}
