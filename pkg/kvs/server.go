package kvs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"unicode"

	"github.com/gorilla/mux"
)

type IntStore struct {
	data map[string]int
	sync.RWMutex
}

type UintStore struct {
	data map[string]uint32
	sync.RWMutex
}

type FloatStore struct {
	data map[string]float32
	sync.RWMutex
}

type StrStore struct {
	data map[string]string
	sync.RWMutex
}

type MapStore struct {
	data map[string]map[string]string
	sync.RWMutex
}

type CommonStore struct {
	ints    *IntStore
	uints   *UintStore
	floats  *FloatStore
	strings *StrStore
	maps    *MapStore

	// naming is hard..., but the common abbreviation for transactions is "txn"
	txnLogger TransactionLogger
}

type cmdResult struct {
	exists bool // rename to deleted?
	val    interface{}
	err    error
}

func newCommonStore() *CommonStore {
	txnLogger, _ := newFileTransactionsLogger("transactions.log")

	return &CommonStore{
		ints:    newIntStore(),
		uints:   newUintStore(),
		floats:  newFloatStore(),
		strings: newStrStore(),
		maps:    newMapStore(),

		txnLogger: txnLogger,
	}
}

func newIntStore() *IntStore {
	return &IntStore{
		data: make(map[string]int),
	}
}

func newUintStore() *UintStore {
	return &UintStore{
		data: make(map[string]uint32),
	}
}

func newFloatStore() *FloatStore {
	return &FloatStore{
		data: make(map[string]float32),
	}
}

func newStrStore() *StrStore {
	return &StrStore{
		data: make(map[string]string),
	}
}

func newMapStore() *MapStore {
	return &MapStore{
		data: make(map[string]map[string]string),
	}
}

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}

func (s *MapStore) put(hashkey string, m map[string]string) *cmdResult {
	s.Lock()
	s.data[hashkey] = m
	s.Unlock()
	return &cmdResult{}
}

func (s *MapStore) get(hashkey string) *cmdResult {
	s.RLock()
	val, exists := s.data[hashkey]
	s.RUnlock()
	if !exists {
		return &cmdResult{err: errorf("Key %s not found in map storage", hashkey)}
	}
	return &cmdResult{val: val}
}

func (s *MapStore) del(hashkey string) *cmdResult {
	s.Lock()
	_, exists := s.data[hashkey]
	delete(s.data, hashkey)
	s.Unlock()
	return &cmdResult{exists: exists}
}

func (s *StrStore) put(key string, val string) *cmdResult {
	s.Lock()
	s.data[key] = val
	s.Unlock()
	return &cmdResult{}
}

func (s *StrStore) get(key string) *cmdResult {
	s.RLock()
	val, exists := s.data[key]
	s.RUnlock()
	if !exists {
		return &cmdResult{err: errorf("Key %s not found in string storage", key)}
	}
	return &cmdResult{val: val}
}

func (s *StrStore) del(key string) *cmdResult {
	s.Lock()
	_, exists := s.data[key]
	delete(s.data, key)
	s.Unlock()
	return &cmdResult{exists: exists}
}

func (s *IntStore) put(key string, val int) *cmdResult {
	s.Lock()
	s.data[key] = val
	s.Unlock()
	return &cmdResult{}
}

func (s *IntStore) get(key string) *cmdResult {
	s.RLock()
	val, exists := s.data[key]
	s.RUnlock()
	if !exists {
		return &cmdResult{err: errorf("Key %s not found in integral storage", key)}
	}
	return &cmdResult{val: val}
}

func (s *IntStore) del(key string) *cmdResult {
	s.Lock()
	_, exists := s.data[key]
	delete(s.data, key)
	s.Unlock()
	return &cmdResult{exists: exists}
}

func (s *FloatStore) get(key string) *cmdResult {
	s.Lock()
	val, exists := s.data[key]
	s.Unlock()
	if !exists {
		return &cmdResult{err: errorf("Key %d not found in floats storage", key)}
	}
	return &cmdResult{val: val}
}

func (s *FloatStore) put(key string, val float32) *cmdResult {
	s.Lock()
	s.data[key] = val
	s.Unlock()
	return &cmdResult{}
}

func (s *FloatStore) del(key string) *cmdResult {
	s.Lock()
	_, exists := s.data[key]
	s.Unlock()
	return &cmdResult{exists: exists}
}

func (store *CommonStore) stringPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if res := store.strings.put(key, string(val)); res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	store.txnLogger.writePutTransaction(key, string(val))
}

func (store *CommonStore) stringGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.strings.get(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusNotFound)
		return
	}
	bytes := []byte(res.val.(string))
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
	store.txnLogger.writeGetTransaction(key)
}

func (store *CommonStore) stringDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.strings.del(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	if res.exists {
		w.Header().Add("Deleted", "true")
	}
	w.WriteHeader(http.StatusNoContent)
	store.txnLogger.writeDeleteTransaction(key)
}

func (store *CommonStore) mapPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashKey := vars["hashkey"]

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
	res := store.maps.put(hashKey, hashMap)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	store.txnLogger.writePutTransaction(hashKey, hashMap)
}

func (store *CommonStore) mapGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashKey := vars["key"]

	res := store.maps.get(hashKey)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	store.txnLogger.writeGetTransaction(hashKey)
	bytes, err := json.Marshal(res.val)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (store *CommonStore) mapDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashKey := vars["key"]

	res := store.maps.del(hashKey)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	if res.exists {
		w.Header().Add("Deleted", "true")
	}
	// TODO: Document this in the architecture manual
	w.WriteHeader(http.StatusNoContent)
	store.txnLogger.writeDeleteTransaction(hashKey)
}

func (store *CommonStore) intPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.Atoi(string(body))
	if err != nil { // Most likely the error will never occur, requires verification
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	store.ints.put(key, val) // omitted
	w.WriteHeader(http.StatusCreated)

	store.txnLogger.writePutTransaction(key, val)
}

func (store *CommonStore) intGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.ints.get(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Conent-Type", "text/plain")
	// w.Header().Add("Content-Length", ...) // added automatically
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", res.val)))

	store.txnLogger.writeGetTransaction(key)
}

func (store *CommonStore) intDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.ints.del(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	if res.exists {
		w.Header().Add("Deleted", "true")
	}
	w.WriteHeader(http.StatusNoContent)
	store.txnLogger.writeDeleteTransaction(key)
}

func (store *CommonStore) floatGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.floats.get(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Conent-Type", "text/plain")
	// w.Header().Add("Content-Length", ...) // added automatically
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", res.val)))

	store.txnLogger.writeGetTransaction(key)
}

func (store *CommonStore) floatPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	val, err := strconv.ParseFloat(string(body), 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError) // float32 parse error
		return
	}
	store.floats.put(key, float32(val))
	w.WriteHeader(http.StatusCreated)
	store.txnLogger.writePutTransaction(key, val)
}

func (store *CommonStore) floatDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	res := store.floats.del(key)
	if res.err != nil {
		http.Error(w, res.err.Error(), http.StatusInternalServerError)
		return
	}
	if res.exists {
		w.Header().Add("Content-Type", "text/plain")
		w.Header().Add("Deleted", "true")
	}
	w.WriteHeader(http.StatusNoContent)
	store.txnLogger.writeDeleteTransaction(key)
}

func (store *CommonStore) echoHandler(w http.ResponseWriter, r *http.Request) {
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

func (store *CommonStore) helloHandler(w http.ResponseWriter, r *http.Request) {
	const helloStr = "Hello from KVS server"
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(helloStr)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(helloStr))
}

type Settings struct {
	Endpoint    string
	CertPemFile string
	KeyPemFile  string
}

func RunServer(settings *Settings) {
	// Replay events from the transactions logger file if the server crashed.
	// We should read all the events first before getting to processing all transactions
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := newCommonStore()
	store.txnLogger.readSavedEvents()

	go store.txnLogger.processEvents(ctx)

	router := mux.NewRouter()

	router.HandleFunc("/api/v1/echo", store.echoHandler).Methods("PUT")
	router.HandleFunc("/api/v1/hello", store.helloHandler).Methods("GET")

	mapRoute := "/api/v1/mapstore/{key:[0-9A-Za-z]+}"
	router.HandleFunc(mapRoute, store.mapPutHandler).Methods("PUT")
	router.HandleFunc(mapRoute, store.mapGetHandler).Methods("GET")
	router.HandleFunc(mapRoute, store.mapDeleteHandler).Methods("DELETE")

	strRoute := "/api/v1/strstore/{key:[0-9A-Za-z]+}"
	router.HandleFunc(strRoute, store.stringPutHandler).Methods("PUT")
	router.HandleFunc(strRoute, store.stringGetHandler).Methods("GET")
	router.HandleFunc(strRoute, store.stringDeleteHandler).Methods("DELETE")

	intRoute := "/api/v1/intstore/{key:[0-9A-Za-z]+}"
	router.HandleFunc(intRoute, store.intPutHandler).Methods("PUT")
	router.HandleFunc(intRoute, store.intGetHandler).Methods("GET")
	router.HandleFunc(intRoute, store.intDeleteHandler).Methods("DELETE")

	if err := http.ListenAndServe(settings.Endpoint, router); err != nil {
		fmt.Printf("Error %v\n", err)
	}
}
