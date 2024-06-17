package kvs

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"unicode"

	"github.com/gorilla/mux"
)

// TODO: Make a set store (storage of unique identifiers)
// If PUT method is issued and the value already exists,
// we shouldn't append a it.
// Support deletion as well.
// Make it sorted?
// Add time storage as well
//
// TODO: Implement handlers table? If horilla mux doesn't do it on its own.

type I64Store struct {
	data map[string]int64
	sync.RWMutex
}

type U64Store struct {
	data map[string]uint64
	sync.RWMutex
}

type F64Store struct {
	data map[string]float64
	sync.RWMutex
}

type StrStore struct {
	data map[string]string
	sync.RWMutex
}

type SliceStore struct {
	data map[string][]string
	sync.RWMutex
}

type MapStore struct {
	data map[string]map[string]string
	sync.RWMutex
}

func newI64Store() *I64Store {
	return &I64Store{
		data: make(map[string]int64),
	}
}

func newU64Store() *U64Store {
	return &U64Store{
		data: make(map[string]uint64),
	}
}

func newF64Store() *F64Store {
	return &F64Store{
		data: make(map[string]float64),
	}
}

func newStrStorage() *StrStore {
	return &StrStore{
		data: make(map[string]string),
	}
}

func newSliceStore() *SliceStore {
	return &SliceStore{
		data: make(map[string][]string),
	}
}

func newMapStorage() *MapStore {
	return &MapStore{
		data: make(map[string]map[string]string),
	}
}

type Settings struct {
	Addr      string
	TLSConfig *tls.Config
}

var transactionLogger, _ = newFileTransactionsLogger("transactions_log.txt")

// NOTE: To avoid having global variables we can declare the handles inside each storage type
// as a member function.
var intStorage = newI64Store()
var uintStorage = newU64Store()
var floatStorage = newF64Store()
var strStorage = newStrStorage()
var slieceStorage = newSliceStore()
var mapStorage = newMapStorage()

// func (s *Storage) put(hashkey, key, value string) {
// 	s.Lock()
// 	defer s.Unlock()

// 	bucket, exists := s.data[hashkey]
// 	if !exists {
// 		m := make(map[string]string)
// 		m[key] = value
// 		s.data[hashkey] = m
// 		return
// 	}
// 	bucket[key] = value
// }

// func (s *Storage) get(hashkey, key string) (string, error) {
// 	s.RLock()
// 	defer s.RUnlock()

// 	bucket, exists := s.data[hashkey]
// 	if !exists {
// 		return "", fmt.Errorf("bucket doesn't exist")
// 	}

// 	value, exists := bucket[key]
// 	if !exists {
// 		return "", fmt.Errorf("key doesn't exist")
// 	}

// 	return value, nil
// }

// func (s *Storage) delete(hashkey, key string) bool {
// 	s.Lock()
// 	defer s.Unlock()

// 	bucket, exists := s.data[hashkey]
// 	if exists {
// 		_, exists := bucket[key]
// 		if exists {
// 			delete(bucket, key)
// 			// delete the bucket itself if it's empty
// 			if len(bucket) == 0 {
// 				delete(s.data, hashkey)
// 			}
// 			return true
// 		}
// 	}
// 	return false
// }

// func (s *Storage) head(hashkey, key string) bool {
// 	s.RLock()
// 	defer s.RUnlock()

// 	if bucket, exists := s.data[hashkey]; exists {
// 		if _, exists := bucket[key]; exists {
// 			return true
// 		}
// 	}
// 	return false
// }

// func getHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)

// 	value, err := storage.get(vars["hashkey"], vars["key"])
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusNotFound)
// 	}

// 	// StatusOK will be written automatically,
// 	// we don't have to call WriteHeader manually
// 	w.Write([]byte(value))
// }

// func putHandler(w http.ResponseWriter, r *http.Request) {
// 	data, err := io.ReadAll(r.Body)
// 	defer r.Body.Close()

// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	// NOTE: If content-type is application/octet-stream, put binaries instead of a string
// 	vars := mux.Vars(r)
// 	storage.put(vars["hashkey"], vars["key"], string(data))

// return the status created error code
// 	w.WriteHeader(http.StatusCreated)
// }

// func deleteHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	if storage.delete(vars["hashkey"], vars["key"]) {
// 		w.Header().Add("deleted", "true")
// 		return
// 	}

// 	w.WriteHeader(http.StatusNotFound)
// }

// Returns a metadata about the resource rather than a resource itself
// func headHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	if storage.head(vars["hashkey"], vars["key"]) {
// 		w.Header().Add("exists", "true")
// 		return
// 	}

// 	w.Header().Add("exists", "false")
// }

// TODO: Try out the request timeout on the client
// Model the scenario when the server receives the requests but hangs for 20s before returning the response.
// Request timeout should be less than that.
// Incr should put a new value if it doesn't exist already.
// func incrHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	key := vars["key"]
// 	// value, exists :=
// }
// NOTE: Storage can be made as an interface

func (m *MapStore) put() {

}

func (m *MapStore) get() (map[string]string, error) {
	return nil, nil
}

func (m *MapStore) del() {

}

func (s *StrStore) put(key string, val string) error {
	s.Lock()
	s.data[key] = val
	s.Unlock()

	return nil
}

func (s *StrStore) get(key string) (string, error) {
	s.RLock()
	val, exists := s.data[key]
	s.RUnlock()

	if !exists {
		return "", fmt.Errorf("key %s doesn't exist", key)
	}

	return val, nil
}

func (s *StrStore) del(key string) (bool, error) {
	exists := false
	s.Lock()
	if _, exists = s.data[key]; exists {
		delete(s.data, key)
	}
	s.Unlock()

	return exists, nil
}

func strStorePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = strStorage.put(key, string(val))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func strStoreGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	val, err := strStorage.get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	bytes := []byte(val)
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func strStoreDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	deleted, _ := strStorage.del(key)
	if deleted {
		w.Header().Add("deleted", "true")
	}
	w.WriteHeader(http.StatusOK)
}

func mapStorePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	hashKey := vars["hashkey"]

	pairs, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_ = hashKey
	_ = pairs
}

// router.HandleFunc(mapRoute, mapStoreGetHandler)
// router.HandleFunc(mapRoute, mapStoreDeleteHandler)

func RunServer(settings *Settings) {
	router := mux.NewRouter()

	router.HandleFunc("/v1/echo", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		buf := make([]byte, r.ContentLength)
		_, err := r.Body.Read(buf)

		if err != nil && err != io.EOF {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(string(val)))
	}).Methods("PUT")

	// v1 is the version of the service
	// route := "/v1/{hashkey}/{key:[0-9A-Za-z]+}"
	// router.HandleFunc(route, getHandler).Methods("GET")
	// router.HandleFunc(route, putHandler).Methods("PUT")
	// router.HandleFunc(route, deleteHandler).Methods("DELETE")
	// router.HandleFunc(route, headHandler).Methods("HEAD")
	// router.HandleFunc("/v1/{key:[0-9A-Za-z]{256}}", incrHandler).Methods("PUT")
	// router.HandleFunc("/v1/{key:[0-9A-Za-z]{256}}/{count:[0-9\\-]+}", incrByHandler).Methods("PUT")

	mapRoute := "/v1/mapstore/{hashkey:[0-9A-Za-z]+}"
	router.HandleFunc(mapRoute, mapStorePutHandler)
	// router.HandleFunc(mapRoute, mapStoreGetHandler)
	// router.HandleFunc(mapRoute, mapStoreDeleteHandler)

	strRoute := "/v1/strstore/{key:[0-9A-Za-z]+}"
	router.HandleFunc(strRoute, strStorePutHandler)
	router.HandleFunc(strRoute, strStoreGetHandler)
	router.HandleFunc(strRoute, strStoreDeleteHandler)

	fmt.Printf("Listening: %s\n", settings.Addr)

	if err := http.ListenAndServe(settings.Addr, router); err != nil {
		fmt.Printf("Error %v\n", err)
	}
}
