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

type Cmd struct {
	// Value to be added to the storage
	args []interface{}
	// Result of Get operation
	result interface{}
	// If the value existed before delete operation, set to true
	deleted bool
	// If operation returned an error,
	// such as value doesn't exist from Get command
	err error
}

func newCmd(args ...interface{}) *Cmd {
	return &Cmd{
		args: args,
	}
}

type baseStorage interface {
	Add(key string, cmd *Cmd) *Cmd
	Get(key string, cmd *Cmd) *Cmd
	Del(key string, cmd *Cmd) *Cmd
}

type IntStorage struct {
	data map[string]int
	sync.RWMutex
}

type UintStorage struct {
	data map[string]uint
	sync.RWMutex
}

type FloatStorage struct {
	data map[string]float32
	sync.RWMutex
}

type StrStorage struct {
	data map[string]string
	sync.RWMutex
}

type MapStorage struct {
	data map[string]map[string]string
	sync.RWMutex
}

type Storage struct {
	// In case we add more things here in the future let's keep it as a sturct
	table map[StorageType]baseStorage
}

type SetStorage MapStorage

func newStorage() *Storage {
	return &Storage{
		table: make(map[StorageType]baseStorage, 5),
	}
}

var globalTransactionLogger *FileTransactionLogger
var globalStorage *Storage

func initStorage() {
	// table[uintStorageKind] = newUintStorage()
	globalStorage = newStorage()
	globalStorage.table[storageTypeInt] = newIntStorage()
	globalStorage.table[storageTypeFloat] = newFloatStorage()
	globalStorage.table[storageTypeString] = newStrStorage()
	globalStorage.table[storageTypeMap] = newMapStorage()
}

func newMapStorage() *MapStorage {
	return &MapStorage{
		data: make(map[string]map[string]string),
	}
}

func newIntStorage() *IntStorage {
	return &IntStorage{
		data: make(map[string]int),
	}
}

func newUintStorage() *UintStorage {
	return &UintStorage{
		data: make(map[string]uint),
	}
}

func newFloatStorage() *FloatStorage {
	return &FloatStorage{
		data: make(map[string]float32),
	}
}

func newStrStorage() *StrStorage {
	return &StrStorage{
		data: make(map[string]string),
	}
}

func (s *MapStorage) Add(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(map[string]string)
	s.Unlock()
	return cmd
}

func (s *MapStorage) Get(hashKey string, cmd *Cmd) *Cmd {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *MapStorage) Del(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	// If the value existed before deletion, cmd.deleted is set to true,
	// false otherwise, so that on the client side we can determine whether the operation modified the storage or not.
	cmd.deleted = exists
	return cmd
}

func (s *StrStorage) Add(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(string)
	s.Unlock()
	return cmd
}

func (s *StrStorage) Get(hashKey string, cmd *Cmd) *Cmd {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *StrStorage) Del(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.deleted = exists
	return cmd
}

func (s *IntStorage) Add(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	s.data[hashKey] = cmd.args[0].(int)
	s.Unlock()
	return cmd
}

func (s *IntStorage) Get(hashKey string, cmd *Cmd) *Cmd {
	s.RLock()
	val, exists := s.data[hashKey]
	s.RUnlock()
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *IntStorage) Del(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.deleted = exists
	return cmd
}

// The response should contain the previous value
func (s *IntStorage) Incr(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	defer s.Unlock()
	val, exists := s.data[hashKey]
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + 1)
	return cmd
}

func (s *IntStorage) IncrBy(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	defer s.Unlock()
	val, exists := s.data[hashKey]
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	s.data[hashKey] = (val + cmd.args[0].(int))
	return cmd
}

func (s *FloatStorage) Add(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	// Consider the precision in the future. Maybe we have to suply the precision
	// together with the value itself
	s.data[hashKey] = float32(cmd.args[0].(float64))
	s.Unlock()
	return cmd
}

func (s *FloatStorage) Get(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	val, exists := s.data[hashKey]
	s.Unlock()
	if !exists {
		cmd.err = errors.NotFoundf("Key %s", hashKey)
		return cmd
	}
	cmd.result = val
	return cmd
}

func (s *FloatStorage) Del(hashKey string, cmd *Cmd) *Cmd {
	s.Lock()
	_, exists := s.data[hashKey]
	delete(s.data, hashKey)
	s.Unlock()
	cmd.deleted = exists
	return cmd
}

func stringAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.table[storageTypeString].Add(key, newCmd(string(val)))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)

	globalTransactionLogger.writeEvent(eventTypeAdd, storageTypeString, key, string(val))
}

func stringGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeString].Get(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	bytes := []byte(cmd.result.(string))
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)

	globalTransactionLogger.writeEvent(eventTypeGet, storageTypeString, key)
}

func stringDeleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeString].Del(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusNoContent)

	globalTransactionLogger.writeEvent(eventTypeDel, storageTypeString, key)
}

func mapAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
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
	cmd := globalStorage.table[storageTypeMap].Add(key, newCmd(hashMap))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)

	globalTransactionLogger.writeEvent(eventTypeAdd, storageTypeMap, key, hashMap)
}

func mapGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeMap].Get(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	defer globalTransactionLogger.writeEvent(eventTypeGet, storageTypeMap, key)

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

func mapDeleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeMap].Del(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusNoContent)

	globalTransactionLogger.writeEvent(eventTypeDel, storageTypeMap, key)
}

func intAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.Atoi(string(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.table[storageTypeInt].Add(key, newCmd(val))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)

	globalTransactionLogger.writeEvent(eventTypeAdd, storageTypeInt, key, val)
}

func intGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeInt].Get(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Add("Conent-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", cmd.result)))

	globalTransactionLogger.writeEvent(eventTypeGet, storageTypeInt, key)
}

func intDeleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeInt].Del(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusNoContent)

	globalTransactionLogger.writeEvent(eventTypeDel, storageTypeInt, key)
}

func intIncrHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	intStorage := globalStorage.table[storageTypeInt].(*IntStorage)
	cmd := intStorage.Incr(key, newCmd())
	if cmd != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	// response body should contain the preivous value
	contents := strconv.FormatInt(cmd.result.(int64), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(contents))

	globalTransactionLogger.writeEvent(eventTypeIncr, storageTypeInt, key)
}

func intIncrByHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	bytes, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.ParseInt(string(bytes), 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	intStorage := globalStorage.table[storageTypeInt].(*IntStorage)
	cmd := intStorage.IncrBy(key, newCmd(val))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// response should contain the previously inserted value
	contents := strconv.FormatInt(cmd.result.(int64), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(contents))

	globalTransactionLogger.writeEvent(eventTypeIncrBy, storageTypeInt, key, val)
}

func floatGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeFloat].Get(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Add("Conent-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%e", cmd.result)))

	globalTransactionLogger.writeEvent(eventTypeGet, storageTypeFloat, key)
}

func floatAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.ParseFloat(string(body), 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := globalStorage.table[storageTypeFloat].Add(key, newCmd(val))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)

	globalTransactionLogger.writeEvent(eventTypeAdd, storageTypeFloat, key, val)
}

func floatDeleteHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	key := mux.Vars(r)["key"]
	cmd := globalStorage.table[storageTypeFloat].Del(key, newCmd())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusNoContent)

	globalTransactionLogger.writeEvent(eventTypeDel, storageTypeFloat, key)
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	// TODO: Experiment with request cancelations
	// select {
	// case <-r.Context().Done():
	// }

	buf, err := io.ReadAll(r.Body)
	defer r.Body.Close()

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

func helloHandler(w http.ResponseWriter, r *http.Request) {
	// Hello endpoint should log transactions, since it doesn't modify the internal storage.
	log.Logger.Info("Endpoint %s, method %s", r.RequestURI, r.Method)

	const helloStr = "Hello from KVS service"
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(helloStr)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(helloStr))
}

func initTransactionLogger(ctx context.Context, transactionLoggerFileName string) error {
	var err error

	globalTransactionLogger, err = newFileTransactionsLogger(transactionLoggerFileName)
	if err != nil {
		return err
	}

	events, errors := globalTransactionLogger.readEvents()

Running:
	for {
		select {
		case event := <-events:
			switch event.Type {
			case eventTypeAdd:
				cmd := globalStorage.table[event.StorageType].Add(event.Key, newCmd(event.Val))
				err = cmd.err

			case eventTypeGet:
				// Get events don't modify the storage anyhow,
				// so probably we don't need to store them anywhere
				cmd := globalStorage.table[event.StorageType].Get(event.Key, newCmd())
				err = cmd.err

			case eventTypeDel:
				cmd := globalStorage.table[event.StorageType].Del(event.Key, newCmd())
				err = cmd.err

			case eventTypeIncr:
				intStorage := globalStorage.table[event.StorageType].(*IntStorage)
				cmd := intStorage.IncrBy(event.Key, newCmd())
				err = cmd.err

			case eventTypeIncrBy:
				intStorage := globalStorage.table[event.StorageType].(*IntStorage)
				cmd := intStorage.Incr(event.Key, newCmd(event.Val))
				err = cmd.err
			}

		case err = <-errors:
			break Running
		}
	}

	if err != nil && err != io.EOF {
		return err
	}

	go globalTransactionLogger.writeEvents(ctx)
	return nil
}

type Settings struct {
	Endpoint           string
	CertPemFile        string
	KeyPemFile         string
	Username           string
	Password           string
	TransactionLogFile string
}

func RunServer(settings *Settings) {
	// Replay events from the transactions logger file if the server crashed.
	// We should read all the events first before getting to processing all transactions
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initStorage()

	if err := initTransactionLogger(ctx, settings.TransactionLogFile); err != nil {
		log.Logger.Panic("Failed to initialize the transaction logger %v", err)
	}

	router := mux.NewRouter()
	// /path/ and /path will trigger the same endpoint
	// router = router.StrictSlash(true)

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

	// https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	httpServer := http.Server{
		Addr:    settings.Endpoint,
		Handler: router,
	}

	serverWaitGroup := sync.WaitGroup{}
	serverWaitGroup.Add(1)
	go func() {
		defer serverWaitGroup.Done()
		log.Logger.Info("Listening %s", settings.Endpoint)
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Logger.Panic("Server error %v", err)
			return
		}
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Logger.Panic("Server shutdown failed")
	}

	serverWaitGroup.Wait()

	log.Logger.Info("Server was closed gracefully")
}
