package kvs

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

type Storage struct {
	data map[string]map[string]string
	sync.RWMutex
}

type Settings struct {
	Addr string
}

var storage = NewStorage()

func NewStorage() *Storage {
	return &Storage{
		data: make(map[string]map[string]string),
	}
}

// If the key is the same, we override the data
func (s *Storage) Put(hashkey, key, value string) {
	s.Lock()
	defer s.Unlock()

	bucket, exists := s.data[hashkey]
	if !exists {
		m := make(map[string]string)
		m[key] = value
		s.data[hashkey] = m
		return
	}
	bucket[key] = value
}

func (s *Storage) Get(hashkey, key string) (string, error) {
	s.RLock()
	defer s.RUnlock()

	bucket, exists := s.data[hashkey]
	if !exists {
		return "", fmt.Errorf("bucket doesn't exist")
	}

	value, exists := bucket[key]
	if !exists {
		return "", fmt.Errorf("key doesn't exist")
	}

	return value, nil
}

func (s *Storage) Delete(hashkey, key string) bool {
	s.Lock()
	defer s.Unlock()

	bucket, exists := s.data[hashkey]
	if exists {
		_, exists := bucket[key]
		if exists {
			delete(bucket, key)
			// delete the bucket itself if it's empty
			if len(bucket) == 0 {
				delete(s.data, hashkey)
			}
			return true
		}
	}
	return false
}

// Check whether a key exists
func (s *Storage) Head(hashkey, key string) bool {
	s.RLock()
	defer s.RUnlock()

	if bucket, exists := s.data[hashkey]; exists {
		if _, exists := bucket[key]; exists {
			return true
		}
	}
	return false
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	value, err := storage.Get(vars["hashkey"], vars["key"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}

	// StatusOK will be written automatically,
	// we don't have to call WriteHeader manually
	w.Write([]byte(value))
}

func PutHandler(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// NOTE: If content-type is application/octet-stream, put binaries instead of a string
	vars := mux.Vars(r)
	storage.Put(vars["hashkey"], vars["key"], string(data))

	// return the status created error code
	w.WriteHeader(http.StatusCreated)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if storage.Delete(vars["hashkey"], vars["key"]) {
		w.Header().Add("deleted", "true")
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

// Returns a metadata about the resource rather than a resource itself
func HeadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if storage.Head(vars["hashkey"], vars["key"]) {
		w.Header().Add("exists", "true")
		return
	}

	w.Header().Add("exists", "false")
}

func RunServer(settings *Settings) {
	router := mux.NewRouter()

	// echo endpoint
	router.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// v1 is the version of the service
	route := "/v1/{hashkey}/{key:[0-9A-Za-z]+}"
	router.HandleFunc(route, GetHandler).Methods("GET")
	router.HandleFunc(route, PutHandler).Methods("PUT")
	router.HandleFunc(route, DeleteHandler).Methods("DELETE")
	router.HandleFunc(route, HeadHandler).Methods("HEAD")

	fmt.Printf("Listening: %s\n", settings.Addr)

	if err := http.ListenAndServe(settings.Addr, router); err != nil {
		fmt.Printf("Error %v\n", err)
	}
}
