package testutil

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/version"
)

type handlerCallback func(w http.ResponseWriter, req *http.Request)

type handler struct {
	method, route string
	cb            handlerCallback
}

type MockServer struct {
	retryCount int
	httpServer *http.Server
	running    bool
	handlers   []*handler
}

func NewMockServer(endpoint string, retryCount int) *MockServer {
	return &MockServer{
		httpServer: &http.Server{
			Addr: endpoint,
		},
		retryCount: retryCount,
		handlers:   make([]*handler, 0),
	}
}

func (s *MockServer) BindHandler(route, method string, handlerCb func(w http.ResponseWriter, req *http.Request)) {
	if s.running {
		log.Logger.Panic("Cannot bind handler, the server is already running")
		return
	}
	s.handlers = append(s.handlers, &handler{method: method, route: route, cb: handlerCb})
}

func (s *MockServer) Start() {
	go func() {
		router := mux.NewRouter().StrictSlash(true)
		subrouter := router.PathPrefix(fmt.Sprintf("/api/%s/", version.GetServiceVersion()))

		s.httpServer.Handler = router

		for _, hd := range s.handlers {
			subrouter.Path(hd.route).HandlerFunc(hd.cb).Methods(hd.method)
		}

		log.Logger.Info("Mock server %v", s.httpServer.Addr)

		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Logger.Panic("ListenAndServe failed %v", err)
			return
		}
	}()
}

// Pass the context directly as an argument to Kill?
func (s *MockServer) Kill() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil && err != context.DeadlineExceeded {
		log.Logger.Panic("Failed to shutdown the server %v", err)
		return
	}
}
