package kvs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/version"
)

// TODO: Implement a throttle pattern on the server side.
// We should limit the amount of requests a client can make to a service
// to 10 requests per second.

type storageI interface {
	Add(key string, cmd *CmdResult) *CmdResult
	Get(key string, cmd *CmdResult) *CmdResult
	Del(key string, CmdResult *CmdResult) *CmdResult
}

type baseStorage struct {
	// Moved to a base class in case we need to add some fields in the future
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

type Storage struct {
	memory map[StorageType]storageI
}

type CmdResult struct {
	kind    StorageType
	args    []interface{}
	result  interface{}
	deleted bool
	err     error
}

func newCmdResult(args ...interface{}) *CmdResult {
	return &CmdResult{
		args: args,
	}
}

func newStorage() *Storage {
	return &Storage{
		memory: make(map[StorageType]storageI),
	}
}

var globalTransactionLogger *FileTransactionLogger
var globalStorage *Storage

func initStorage() {
	globalStorage = newStorage()
	globalStorage.memory[storageInt] = newIntStorage()
	globalStorage.memory[storageFloat] = newFloatStorage()
	globalStorage.memory[storageString] = newStrStorage()
	globalStorage.memory[storageMap] = newMapStorage()
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
	cmd.kind = storageMap
	return cmd
}

func (s *MapStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.kind = storageMap
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
	cmd.kind = storageMap
	// If the value existed before deletion, cmd.deleted is set to true,
	// false otherwise, so that on the client side we can determine whether the operation modified the storage or not.
	cmd.deleted = exists
	return cmd
}

func (s *StrStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(string)
	s.Unlock()
	cmd.kind = storageString
	return cmd
}

func (s *StrStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.kind = storageString
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
	cmd.kind = storageString
	cmd.deleted = exists
	return cmd
}

func (s *IntStorage) Add(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(int)
	s.Unlock()
	cmd.kind = storageInt
	return cmd
}

func (s *IntStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	cmd.kind = storageInt
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
	cmd.kind = storageInt
	cmd.deleted = exists
	return cmd
}

func (s *IntStorage) Incr(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	defer s.Unlock()
	cmd.kind = storageInt
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
	cmd.kind = storageInt
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
	cmd.kind = storageFloat
	return cmd
}

func (s *FloatStorage) Get(hashKey string, cmd *CmdResult) *CmdResult {
	s.Lock()
	val, exists := s.data[hashKey]
	s.Unlock()
	cmd.kind = storageFloat
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
	cmd.kind = storageFloat
	cmd.deleted = exists
	return cmd
}

func stringAddHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	val, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.memory[storageString].Add(key, newCmdResult(string(val)))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventAdd, storageString, key, string(val))

	w.WriteHeader(http.StatusCreated)
}

func stringGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageString].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	globalTransactionLogger.writeEvent(eventGet, storageString, key)

	bytes := []byte(cmd.result.(string))
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func stringDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageString].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventDel, storageString, key)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}

	w.WriteHeader(http.StatusNoContent)
}

func mapAddHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hashMap := make(map[string]string)
	err = json.Unmarshal(body, &hashMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.memory[storageMap].Add(key, newCmdResult(hashMap))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventAdd, storageMap, key, hashMap)

	w.WriteHeader(http.StatusCreated)
}

func mapGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageMap].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	globalTransactionLogger.writeEvent(eventGet, storageMap, key)

	bytes, err := json.Marshal(cmd.result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func mapDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageMap].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventDel, storageMap, key)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusNoContent)
}

func intAddHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.Atoi(string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.memory[storageInt].Add(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventAdd, storageInt, key, val)
	w.WriteHeader(http.StatusCreated)
}

func intGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageInt].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	globalTransactionLogger.writeEvent(eventGet, storageInt, key)

	w.Header().Add("Conent-Type", "text/plain")

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", cmd.result)))
}

func intDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageInt].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventDel, storageInt, key)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}

	w.WriteHeader(http.StatusNoContent)
}

func intIncrHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	intStorage := globalStorage.memory[storageInt].(*IntStorage)
	cmd := intStorage.Incr(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventIncr, storageInt, key)

	// response body should contain the preivous value
	contents := strconv.FormatInt(int64(cmd.result.(int)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(contents))
}

func intIncrByHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	bytes, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.ParseInt(string(bytes), 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	intStorage := globalStorage.memory[storageInt].(*IntStorage)
	cmd := intStorage.IncrBy(key, newCmdResult(int(val)))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventIncrBy, storageInt, key, val)

	// response should contain the previously inserted value
	contents := strconv.FormatInt(int64(cmd.result.(int)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(contents))
}

func floatGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageFloat].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	globalTransactionLogger.writeEvent(eventGet, storageFloat, key)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%e", cmd.result)))
}

func floatAddHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.ParseFloat(string(body), 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.memory[storageFloat].Add(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	globalTransactionLogger.writeEvent(eventAdd, storageFloat, key, val)

	w.WriteHeader(http.StatusCreated)
}

func floatDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := globalStorage.memory[storageFloat].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	globalTransactionLogger.writeEvent(eventDel, storageFloat, key)

	w.WriteHeader(http.StatusNoContent)
}

func uintAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 PUT endpoint is not implemented yet")
}

func uintGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 GET endpoint is not implemented yet")
}

func uintDelHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 DELETE endpoint is not implemented yet")
}

func delKeyHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	for _, storage := range globalStorage.memory {
		cmd := storage.Del(key, newCmdResult())
		if cmd.deleted {
			w.Header().Add("Deleter", "1")
			globalTransactionLogger.writeEvent(eventDel, cmd.kind, key)
			break
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func echoHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	buf, err := io.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert lowercase letters into uppercase letters and vice-versa
	val := []rune(string(buf))
	for i := 0; i < len(val); i++ {
		if unicode.IsLetter(val[i]) {
			if unicode.IsLower(val[i]) {
				val[i] = unicode.ToUpper(val[i])
				continue
			}
			val[i] = unicode.ToLower(val[i])
		}
	}
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(val)))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(string(val)))
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	const helloStr = "Hello from KVS service"
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(helloStr)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(helloStr))
}

func logOnEndpointHit(reqURI, method, remoteAddr string) {
	log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", reqURI, method, remoteAddr)
}

func initTransactionLogger(ctx context.Context, waitGroup *sync.WaitGroup, transactionLoggerFileName string) error {
	var err error
	var event Event

	globalTransactionLogger, err = newFileTransactionsLogger(transactionLoggerFileName)
	if err != nil {
		return err
	}

	events, errors := globalTransactionLogger.readEvents()

	for {
		select {
		case event = <-events:
			switch event.Type {
			case eventAdd:
				cmd := globalStorage.memory[event.StorageType].Add(event.Key, newCmdResult(event.Val))
				err = cmd.err

			case eventGet:
				// Get events don't modify the storage anyhow,
				// so probably we don't need to store them anywhere
				cmd := globalStorage.memory[event.StorageType].Get(event.Key, newCmdResult())
				err = cmd.err

			case eventDel:
				cmd := globalStorage.memory[event.StorageType].Del(event.Key, newCmdResult())
				err = cmd.err

			case eventIncr:
				intStorage := globalStorage.memory[event.StorageType].(*IntStorage)
				cmd := intStorage.IncrBy(event.Key, newCmdResult())
				err = cmd.err

			case eventIncrBy:
				intStorage := globalStorage.memory[event.StorageType].(*IntStorage)
				cmd := intStorage.Incr(event.Key, newCmdResult(event.Val))
				err = cmd.err
			}

		case err = <-errors:
			if err != io.EOF {
				return err
			}
			// In case the server terminates, we want to make sure,
			// that we have written all the pending events to the transaction log file.
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				globalTransactionLogger.writeEvents(ctx)
			}()
			return nil
		}

		if err != nil {
			return err
		}

		log.Logger.Info("Read %s, id %d, key %s, storage %s",
			event.Type.toStr(),
			event.Id,
			event.Key,
			event.StorageType.toStr(),
		)

		event = Event{}
	}
}

type Settings struct {
	Endpoint    string
	CertPemFile string
	KeyPemFile  string
	Username    string
	Password    string
	// specify transaction logger type
	// either a database, or a transaction log file
	TransactionLogFile string
}

type Server struct {
	*http.Server
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	txnLogger TransactionLogger
}

func RunServer(settings *Settings) {
	initStorage()

	// Replay events from the transactions logger file if the server crashed.
	// We should read all the events first before getting to processing all transactions
	serverShutdownCtx, serverCancelFunc := context.WithCancel(context.Background())
	waitGroup := sync.WaitGroup{}
	if err := initTransactionLogger(serverShutdownCtx, &waitGroup, settings.TransactionLogFile); err != nil {
		log.Logger.Panic("Failed to initialize the transaction logger %v", err)
	}

	router := mux.NewRouter().StrictSlash(true)
	subrouter := router.PathPrefix(fmt.Sprintf("/api/%s/", version.GetServiceVersion())).Subrouter()

	// NOTE: The echo endpoint should be bound to GET method and contain a body,
	// even though it violates the rules of REST api.
	// Since we don't store the received string anywhere on the server side.
	// That will simplify error processing on the client side.
	subrouter.Path("/echo").HandlerFunc(echoHandler).Methods("GET")
	subrouter.Path("/hello").HandlerFunc(helloHandler).Methods("GET")

	subrouter.Path("/mapadd/{key:[0-9A-Za-z_]+}").HandlerFunc(mapAddHandler).Methods("PUT")
	subrouter.Path("/mapget/{key:[0-9A-Za-z_]+}").HandlerFunc(mapGetHandler).Methods("GET")
	subrouter.Path("/mapdel/{key:[0-9A-Za-z_]+}").HandlerFunc(mapDeleteHandler).Methods("DELETE")

	subrouter.Path("/stradd/{key:[0-9A-Za-z_]+}").HandlerFunc(stringAddHandler).Methods("PUT")
	subrouter.Path("/strget/{key:[0-9A-Za-z_]+}").HandlerFunc(stringGetHandler).Methods("GET")
	subrouter.Path("/strdel/{key:[0-9A-Za-z_]+}").HandlerFunc(stringDeleteHandler).Methods("DELETE")

	subrouter.Path("/intadd/{key:[0-9A-Za-z_]+}").HandlerFunc(intAddHandler).Methods("PUT")
	subrouter.Path("/intget/{key:[0-9A-Za-z_]+}").HandlerFunc(intGetHandler).Methods("GET")
	subrouter.Path("/intdel/{key:[0-9A-Za-z_]+}").HandlerFunc(intDeleteHandler).Methods("DELETE")
	subrouter.Path("/intincr/{key:[0-9A-Za-z_]+}").HandlerFunc(intIncrHandler).Methods("PUT")
	subrouter.Path("/intincrby/{key:[0-9A-Za-z_]+}").HandlerFunc(intIncrByHandler).Methods("PUT")

	subrouter.Path("/floatadd/{key:[0-9A-Za-z_]+}").HandlerFunc(floatAddHandler).Methods("PUT")
	subrouter.Path("/floatget/{key:[0-9A-Za-z_]+}").HandlerFunc(floatGetHandler).Methods("GET")
	subrouter.Path("/floatdel/{key:[0-9A-Za-z_]+}").HandlerFunc(floatDeleteHandler).Methods("DELETE")

	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(uintAddHandler).Methods("PUT")
	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(uintGetHandler).Methods("GET")
	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(uintDelHandler).Methods("DELETE")

	// Endpoint to delete a key from any type of storage
	subrouter.Path("/del/{key:[0-9A-Za-z_]+}").HandlerFunc(delKeyHandler).Methods("DELETE")

	// https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	httpServer := http.Server{
		Addr:    settings.Endpoint,
		Handler: router,
	}

	// kill the server endpoint. Any method would work
	subrouter.Path("/kill").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Logger.Info("Killing the server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
		defer cancel()
		// TODO: The programm should wait for the Shutdown function to return before exiting.
		if err := httpServer.Shutdown(shutdownCtx); err != nil && err != context.DeadlineExceeded {
			log.Logger.Error("Server shutdown failed %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	waitGroup.Add(1)
	go func() {
		defer serverCancelFunc()
		defer waitGroup.Done()
		log.Logger.Info("Listening %s", settings.Endpoint)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Logger.Panic("Server terminated abnormally %v", err)
			return
		}
	}()
	waitGroup.Wait()

	log.Logger.Info("Server was closed gracefully")
}
