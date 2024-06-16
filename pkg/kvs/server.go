package kvs

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"unicode"

	"github.com/gorilla/mux"
)

type Storage struct {
	data map[string]map[string]string
	sync.RWMutex
}

type Settings struct {
	Addr string
}

var storage = newStorage()

func newStorage() *Storage {
	return &Storage{
		data: make(map[string]map[string]string),
	}
}

// If the key is the same, we override the data
func (s *Storage) put(hashkey, key, value string) {
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

func (s *Storage) get(hashkey, key string) (string, error) {
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

func (s *Storage) delete(hashkey, key string) bool {
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

func (s *Storage) head(hashkey, key string) bool {
	s.RLock()
	defer s.RUnlock()

	if bucket, exists := s.data[hashkey]; exists {
		if _, exists := bucket[key]; exists {
			return true
		}
	}
	return false
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	value, err := storage.get(vars["hashkey"], vars["key"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}

	// StatusOK will be written automatically,
	// we don't have to call WriteHeader manually
	w.Write([]byte(value))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// NOTE: If content-type is application/octet-stream, put binaries instead of a string
	vars := mux.Vars(r)
	storage.put(vars["hashkey"], vars["key"], string(data))

	// return the status created error code
	w.WriteHeader(http.StatusCreated)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if storage.delete(vars["hashkey"], vars["key"]) {
		w.Header().Add("deleted", "true")
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

// Returns a metadata about the resource rather than a resource itself
func headHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if storage.head(vars["hashkey"], vars["key"]) {
		w.Header().Add("exists", "true")
		return
	}

	w.Header().Add("exists", "false")
}

func incrHandler(w http.ResponseWriter, r *http.Request) {

}

func incrByHandler(w http.ResponseWriter, r *http.Request) {

}

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
	route := "/v1/{hashkey}/{key:[0-9A-Za-z]+}"
	router.HandleFunc(route, getHandler).Methods("GET")
	router.HandleFunc(route, putHandler).Methods("PUT")
	router.HandleFunc(route, deleteHandler).Methods("DELETE")
	router.HandleFunc(route, headHandler).Methods("HEAD")
	router.HandleFunc("/v1/{key:[0-9A-Za-z]{256}}", incrHandler).Methods("PUT")
	router.HandleFunc("/v1/{key:[0-9A-Za-z]{256}}/{count:[0-9\\-]+}", incrByHandler).Methods("PUT")

	fmt.Printf("Listening: %s\n", settings.Addr)

	if err := http.ListenAndServe(settings.Addr, router); err != nil {
		fmt.Printf("Error %v\n", err)
	}
}
