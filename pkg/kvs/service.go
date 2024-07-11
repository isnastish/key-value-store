package kvs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
	"unicode"

	"github.com/gorilla/mux"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/serviceinfo"
)

// TODO: Implement a throttle pattern on the server side.
// We should limit the amount of requests a client can make to a service
// to 10 requests per second.

// NOTE: Instead of passing nil when making GET/DELETE transactions, introduce separate functions
// writeGetTransaction(), writeDeleteTransaction() and only pass the information, (key and a storageType)

type ServiceSettings struct {
	Endpoint    string
	CertPemFile string
	KeyPemFile  string
	Username    string
	Password    string

	// transactions
	TxnFilePath          string
	TransactionsDisabled bool
	TxnLoggerType
}

type HandlerCallback func(w http.ResponseWriter, req *http.Request)

type RPCHandler struct {
	method   string
	funcName string
	cb       HandlerCallback
}

type Service struct {
	*http.Server
	settings    *ServiceSettings
	rpcHandlers []*RPCHandler
	storage     map[StorageType]Storage
	txnLogger   TansactionLogger
	running     bool
}

func (s *Service) stringAddHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	val, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := s.storage[storageTypeString].Add(key, newCmdResult(string(val)))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeTransaction(eventAdd, storageTypeString, key, string(val))

	w.WriteHeader(http.StatusOK)
}

func (s *Service) stringGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeString].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	s.writeTransaction(eventGet, storageTypeString, key, nil)

	bytes := []byte(cmd.result.(string))
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (s *Service) stringDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeString].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeTransaction(eventDel, storageTypeString, key, nil)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Service) mapAddHandler(w http.ResponseWriter, req *http.Request) {
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
	cmd := s.storage[storageTypeMap].Add(key, newCmdResult(hashMap))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventAdd, storageTypeMap, key, hashMap)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) mapGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeMap].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	s.writeTransaction(eventGet, storageTypeMap, key, nil)

	// NOTE: Maybe instead of transferring a stream of bytes, we could send the data
	// for the map in a json format? The content-type would have to be changed to application/json
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

func (s *Service) mapDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeMap].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventDel, storageTypeMap, key, nil)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) intAddHandler(w http.ResponseWriter, req *http.Request) {
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
	cmd := s.storage[storageTypeInt].Add(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventAdd, storageTypeInt, key, val)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) intGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeInt].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	s.writeTransaction(eventGet, storageTypeInt, key, nil)

	w.Header().Add("Conent-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", cmd.result)))
}

func (s *Service) intDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeInt].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventDel, storageTypeInt, key, nil)

	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) intIncrHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	intStorage := s.storage[storageTypeInt].(*IntStorage)
	cmd := intStorage.Incr(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventIncr, storageTypeInt, key, nil)

	// response body should contain the preivous value
	contents := strconv.FormatInt(int64(cmd.result.(int)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(contents))
}

func (s *Service) intIncrByHandler(w http.ResponseWriter, req *http.Request) {
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
	intStorage := s.storage[storageTypeInt].(*IntStorage)
	cmd := intStorage.IncrBy(key, newCmdResult(int(val)))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventIncrBy, storageTypeInt, key, val)

	// response should contain the previously inserted value
	contents := strconv.FormatInt(int64(cmd.result.(int)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(contents))
}

func (s *Service) floatGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeFloat].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}
	s.writeTransaction(eventGet, storageTypeFloat, key, nil)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%e", cmd.result)))
}

func (s *Service) floatAddHandler(w http.ResponseWriter, req *http.Request) {
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
	cmd := s.storage[storageTypeFloat].Add(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeTransaction(eventAdd, storageTypeFloat, key, val)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) floatDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[storageTypeFloat].Del(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	if cmd.deleted {
		w.Header().Add("Deleted", "1")
	}
	s.writeTransaction(eventDel, storageTypeFloat, key, nil)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) uintAddHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 PUT endpoint is not implemented yet")
}

func (s *Service) uintGetHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 GET endpoint is not implemented yet")
}

func (s *Service) uintDelHandler(w http.ResponseWriter, r *http.Request) {
	log.Logger.Info("Uint32 DELETE endpoint is not implemented yet")
}

func (s *Service) delKeyHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	for _, storage := range s.storage {
		cmd := storage.Del(key, newCmdResult())
		if cmd.deleted {
			w.Header().Add("Deleter", "1")
			s.writeTransaction(eventDel, cmd.storageType, key, nil)
			break
		}
	}
	w.WriteHeader(http.StatusOK)
}

func echo(param string) string {
	res := []rune(param)
	for i := 0; i < len(res); i++ {
		if unicode.IsLetter(res[i]) {
			if unicode.IsLower(res[i]) {
				res[i] = unicode.ToUpper(res[i])
				continue
			}
			res[i] = unicode.ToLower(res[i])
		}
	}
	return string(res)
}

func hello() string {
	return fmt.Sprintf("Hello from %s:%s service!", info.ServiceName(), info.ServiceVersion())
}

func fibo(n int) int {
	if n == 0 {
		return 0
	}
	if n == 1 || n == 2 {
		return 1
	}
	return fibo(n-1) + fibo(n-2)
}

func fiboHandler(w http.ResponseWriter, req *http.Request) {
	// Example URL: http://127.0.0.1:5000/kvs/v1-0-0/fibo?n=12
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	path, err := req.URL.Parse(req.RequestURI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	query, err := url.ParseQuery(path.RawQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !path.Query().Has("n") {
		http.Error(w, "Invalid request syntax, {n} query parameter is not found", http.StatusBadRequest)
		return
	}

	n, _ := strconv.Atoi(query["n"][0])
	result := fibo(n)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(strconv.Itoa(result)))
}

func echoHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	buf, err := io.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	val := echo(string(buf))

	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(val)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(val))
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	res := hello()

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprint(len(res)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func logOnEndpointHit(reqURI, method, remoteAddr string) {
	log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", reqURI, method, remoteAddr)
}

func (s *Service) BindRPCHandler(method, funcName string, callback HandlerCallback) {
	if s.running {
		log.Logger.Error("Failed to bind {%s} RPC, service is already running", funcName)
		return
	}

	if callback == nil {
		log.Logger.Error("Failed to bind {%s} RPC, handler cannot be nil", funcName)
		return
	}

	s.rpcHandlers = append(
		s.rpcHandlers,
		&RPCHandler{method: method, funcName: funcName, cb: callback},
	)
}

func (s *Service) processSavedTransactions() error {
	var err error
	var event Event

	events, errors := s.txnLogger.readEvents()

	for {
		select {
		case event = <-events:
			switch event.Type {
			case eventAdd:
				cmd := s.storage[event.StorageType].Add(event.Key, newCmdResult(event.Val))
				err = cmd.err

			case eventGet:
				// Get events don't modify the storage anyhow,
				// so probably we don't need to store them anywhere
				cmd := s.storage[event.StorageType].Get(event.Key, newCmdResult())
				err = cmd.err

			case eventDel:
				cmd := s.storage[event.StorageType].Del(event.Key, newCmdResult())
				err = cmd.err

			case eventIncr:
				intStorage := s.storage[event.StorageType].(*IntStorage)
				cmd := intStorage.IncrBy(event.Key, newCmdResult())
				err = cmd.err

			case eventIncrBy:
				intStorage := s.storage[event.StorageType].(*IntStorage)
				cmd := intStorage.Incr(event.Key, newCmdResult(event.Val))
				err = cmd.err
			}

		case err = <-errors:
			if err != io.EOF {
				return err
			}
			return nil
		}

		if err != nil {
			return err
		}

		log.Logger.Info("Read %s, id %d, key %s, storage %d",
			eventToStr[event.Type],
			event.Id,
			event.Key,
			event.StorageType,
		)
		event = Event{}
	}
}

func (s *Service) writeTransaction(evenType EventType, storageType StorageType, key string, value interface{}) {
	if !s.settings.TransactionsDisabled {
		s.writeTransaction(eventGet, storageTypeString, key, value)
	}
}

func NewService(settings *ServiceSettings) *Service {
	var logger TansactionLogger
	var err error

	switch settings.TxnLoggerType {
	case TxnLoggerTypeFile:
		logger, err = newFileTransactionsLogger(settings.TxnFilePath)
		if err != nil {
			log.Logger.Fatal("Failed to init file transaction logger %v", err)
		}

	case TxnLoggerTypeDB:
		logger, err = newDBTransactionLogger(PostgresSettings{
			host:     "localhost",
			port:     5432,
			dbName:   "postgres",
			userName: "postgres",
			userPwd:  "12345",
		})
		if err != nil {
			log.Logger.Fatal("Failed to init DB transaction logger %v", err)
		}
	}

	// https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	service := &Service{
		Server: &http.Server{
			Addr:         settings.Endpoint,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		},
		settings:    settings,
		rpcHandlers: make([]*RPCHandler, 0),
		storage:     make(map[StorageType]Storage),
		txnLogger:   logger,
	}

	service.storage[storageTypeInt] = newIntStorage()
	service.storage[storageTypeFloat] = newFloatStorage()
	service.storage[storageTypeString] = newStrStorage()
	service.storage[storageTypeMap] = newMapStorage()

	// NOTE: This has to be executed after both transaction logger AND the storage is initialized
	if err := service.processSavedTransactions(); err != nil {
		log.Logger.Fatal("Failed to fetch saved transactions %v", err)
	}

	service.BindRPCHandler("POST", "echo", echoHandler)
	service.BindRPCHandler("POST", "hello", helloHandler)
	service.BindRPCHandler("POST", "fibo", fiboHandler)

	return service
}

func (s *Service) Run() {
	if s.running {
		log.Logger.Error("Service is already running")
		return
	}

	s.running = true

	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.txnLogger.processTransactions(shutdownCtx)
	}()

	router := mux.NewRouter().StrictSlash(true)
	// router.Path()
	subrouter := router.PathPrefix(fmt.Sprintf("/%s/%s/", info.ServiceName(), info.ServiceVersion())).Subrouter()

	// Bind all rpc handlers
	for _, hd := range s.rpcHandlers {
		subrouter.Path("/" + hd.funcName).HandlerFunc(hd.cb).Methods(hd.method)
	}

	subrouter.Path("/mapadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapAddHandler).Methods("PUT")
	subrouter.Path("/mapget/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapGetHandler).Methods("GET")
	subrouter.Path("/mapdel/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapDeleteHandler).Methods("DELETE")

	subrouter.Path("/stradd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringAddHandler).Methods("PUT")
	subrouter.Path("/strget/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringGetHandler).Methods("GET")
	subrouter.Path("/strdel/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringDeleteHandler).Methods("DELETE")

	subrouter.Path("/intadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intAddHandler).Methods("PUT")
	subrouter.Path("/intget/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intGetHandler).Methods("GET")
	subrouter.Path("/intdel/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intDeleteHandler).Methods("DELETE")
	subrouter.Path("/intincr/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intIncrHandler).Methods("PUT")
	subrouter.Path("/intincrby/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intIncrByHandler).Methods("PUT")

	subrouter.Path("/floatadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatAddHandler).Methods("PUT")
	subrouter.Path("/floatget/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatGetHandler).Methods("GET")
	subrouter.Path("/floatdel/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatDeleteHandler).Methods("DELETE")

	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintAddHandler).Methods("PUT")
	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintGetHandler).Methods("GET")
	subrouter.Path("/uintadd/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintDelHandler).Methods("DELETE")

	// Endpoint to delete a key from any type of storage
	subrouter.Path("/del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.delKeyHandler).Methods("DELETE")

	s.Server.Handler = router

	log.Logger.Info("%s:%s service is running %s", info.ServiceName(), info.ServiceVersion(), s.settings.Endpoint)
	if err := s.Server.ListenAndServe(); err != http.ErrServerClosed {
		log.Logger.Error("Server terminated abnormally %v", err)
	} else {
		log.Logger.Info("Server was closed gracefully")
	}

	// Put both these fucns into a defer statement
	cancel()
	wg.Wait()
}
