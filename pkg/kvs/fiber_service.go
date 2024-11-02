// NOTE: This is an experimental module using fiber
// Once the implementation is tested properly, we could do the migration
package kvs

import (
	// TODO: This is not the fastest json encoding/decoding library
	// Use fiber docs as a reference to determine which libraries could be used
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"

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

func (s *FiberService) mapPutHandler(ctx *fiber.Ctx) error {
	key := ctx.Params("key")

	log.Logger.Info("key: %s", key)

	// TODO: We should get the lengths from the content type
	// in a request header
	if len(ctx.Body()) == 0 {
		return fiber.NewError(fiber.StatusInternalServerError, "request body is empty")
	}

	data := make([]byte, len(ctx.Body()))
	copy(data, ctx.Body())

	hashMap := make(map[string]string)
	err := json.Unmarshal(data, &hashMap)
	if err != nil {
		log.Logger.Error("Failed to unmarshal the body %v", err)
		return fiber.NewError(fiber.StatusInternalServerError, "failed to unmarshal request body")
	}

	cmd := s.storage[apitypes.StorageMap].Put(key, newCmdResult(hashMap))
	if cmd.err != nil {
		log.Logger.Error("Failed to put into map storage")
		return fiber.NewError(fiber.StatusInternalServerError, cmd.err.Error())
	}

	// TODO: Handle transactions

	// s.sendTransaction(&apitypes.Transaction{
	// 	TxnType:     apitypes.TransactionPut,
	// 	StorageType: apitypes.StorageMap,
	// 	Key:         key,
	// 	Data:        hashMap,
	// })

	return ctx.SendStatus(fiber.StatusOK)
}

func (s *FiberService) mapGetHandler(ctx *fiber.Ctx) error {
	key := ctx.Params("key")

	cmd := s.storage[apitypes.StorageMap].Get(key, newCmdResult())
	if cmd.err != nil {
		return fiber.NewError(fiber.StatusNotFound, cmd.err.Error())
	}

	// TODO: Make a transaction to the transaction service

	// s.sendTransaction(&apitypes.Transaction{
	// 	Timestamp:   time.Now(),
	// 	TxnType:     apitypes.TransactionGet,
	// 	StorageType: apitypes.StorageMap,
	// 	Key:         key,
	// })

	bytes, err := json.Marshal(cmd.result)
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	if _, err := ctx.Write(bytes); err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, "failed to write response body")
	}

	ctx.Set("Content-Type", "application/octet-stream")
	ctx.Set("Content-Length", strconv.Itoa(len(bytes)))

	return ctx.SendStatus(fiber.StatusOK)
}

func (s *FiberService) mapDeleteHandler(ctx *fiber.Ctx) error {
	key := ctx.Params("key")

	cmd := s.storage[apitypes.StorageMap].Del(key, newCmdResult())
	if cmd.result.(bool) {
		// TODO: Make a transaction
		// s.sendTransaction(&apitypes.Transaction{
		// 	Timestamp:   time.Now(),
		// 	TxnType:     apitypes.TransactionDel,
		// 	StorageType: apitypes.StorageMap,
		// 	Key:         key,
		// })

		ctx.Set("Deleted", "true")
	}

	return ctx.SendStatus(fiber.StatusOK)
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		log.Logger.Info("endpoint: %s, method: %s, remoteAddr: %s", req.RequestURI, req.Method, req.RemoteAddr)
		next.ServeHTTP(w, req)
	})
}

func (s *FiberService) Serve() error {
	s.app.Use(adaptor.HTTPMiddleware(logMiddleware))

	api := s.app.Group(info.ServiceName())
	rootApi := api.Group(info.ServiceVersion())

	// TODO: Figure out how to use regular expressions properly
	// Maybe it would make more sense to specify key directly?
	// Regular expression doesn't function the way it should
	// Once regular expression is working, we should specify it
	// keyRegexp := ":key<regex(0-9A-Za-z_)>"
	// _ = keyRegexp

	rootApi.Put("/map-put/:key", s.mapPutHandler)
	rootApi.Get("/map-get/:key", s.mapGetHandler)
	rootApi.Delete("/map-del/:key", s.mapDeleteHandler)

	s.running = true

	log.Logger.Info("Fiber service is running on port: %s", "0.0.0.0:8080")

	if err := s.app.Listen(":8080"); err != nil {
		return fmt.Errorf("fiber: failed to listen on port :8080 %v", err)
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
