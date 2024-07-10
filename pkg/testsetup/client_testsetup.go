package testsetup

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/serviceinfo"
)

type handler struct {
	method, route string
	cb            func(w http.ResponseWriter, req *http.Request)
}

type MockServer struct {
	httpServer *http.Server
	running    bool
	handlers   []*handler
}

func NewMockServer(endpoint string) *MockServer {
	return &MockServer{
		httpServer: &http.Server{
			Addr: endpoint,
		},
		handlers: make([]*handler, 0),
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
		subrouter := router.PathPrefix(fmt.Sprintf("/%s/%s/", info.ServiceName(), info.ServiceVersion()))

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

func (s *MockServer) Kill() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil && err != context.DeadlineExceeded {
		log.Logger.Panic("Failed to shutdown the server %v", err)
		return
	}
}

var kvsServiceImage = "kvs:latest"

func doesKVSDockerImageExist() (bool, error) {
	exists := false
	cmd := exec.Command("docker", "inspect", "--type=image", kvsServiceImage)
	stdout, err := cmd.Output()
	if err != nil {
		return exists, err
	}

	if strings.Contains(string(stdout), "No such image") {
		return exists, nil
	}
	exists = true

	return exists, nil
}

func StartKVSServiceContainer() (bool, error) {
	tearDown := false
	exists, err := doesKVSDockerImageExist()
	if err != nil {
		return tearDown, err
	}

	if !exists {
		log.Logger.Fatal("Image %s doesn't exist", kvsServiceImage)
	}

	expectedOutput := "service is running"
	return startDockerContainer(expectedOutput, "docker", "run", "--rm", "--publish", "8080:8080" /*host:container*/, "--name", "kvs-service", kvsServiceImage)
}

func KillKVSServiceContainer() {
	killDockerContainer("docker", "rm", "--force", "kvs-service")
}
