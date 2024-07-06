package testutil

import (
	"context"
	"fmt"
	"io"
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

// TODO: Accept the port
func StartKVSServiceContainer() (bool, error) {
	kill := false

	exists, err := doesKVSDockerImageExist()
	if err != nil {
		return kill, err
	}

	if !exists {
		log.Logger.Fatal("Image %s doesn't exist", kvsServiceImage)
	}

	cmd := exec.Command("docker", "run", "--rm", "--publish", "8080:8080", "--name", "kvs-service", kvsServiceImage)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return kill, err
	}
	defer stdout.Close()

	if err := cmd.Start(); err != nil {
		return kill, err
	}

	kill = true // kill the container running the kvs-service

	// Wait at maximum three minutes for the container to boot up
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var strBuilder strings.Builder

	readBuf := make([]byte, 256)
	for {
		select {
		case <-ctx.Done():
			return kill, ctx.Err()
		default:
		}

		n, err := stdout.Read(readBuf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Logger.Fatal("reached end of stdout %v", err)
		}
		if n > 0 {
			outStr := string(readBuf[:n])
			log.Logger.Info(outStr)
			strBuilder.WriteString(outStr)
			if strings.Contains(strBuilder.String(), "service is running") {
				time.Sleep(500 * time.Millisecond)
				break
			}
		}
	}
	return kill, nil
}

func KillKVSServiceContainer() {
	log.Logger.Info("Killing docker container")
	cmd := exec.Command("docker", "rm", "--force", "kvs-service")
	if err := cmd.Run(); err != nil {
		log.Logger.Error("Failed to kill kvs-service container")
		return
	}
	log.Logger.Info("Killed kvs-service container")
}
