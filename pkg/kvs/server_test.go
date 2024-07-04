package kvs

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/isnastish/kvs/pkg/serviceinfo"
)

var endpoint = "127.0.0.1:5000"

func TestTrailingSlash(t *testing.T) {
	defer goleak.VerifyNone(t)

	settings := Settings{Endpoint: endpoint, TransactionLogFile: "transactions.bin"}

	service := NewKVSService(&settings)
	defer os.Remove(settings.TransactionLogFile)

	go service.Run()
	defer service.Kill()

	// Wait a bit for the service to spin up
	time.Sleep(200 * time.Millisecond)

	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/%s/%s", endpoint, info.ServiceName(), info.ServiceVersion()))
	withTrailingSlashURL := baseURL.JoinPath("/echo/")
	// withoutTrailingSlashURL := baseURL.JoinPath("/echo")

	client := http.Client{}
	req, err := http.NewRequest(
		http.MethodGet,
		withTrailingSlashURL.String(),
		bytes.NewBufferString("echo ECHO echo"),
	)
	assert.True(t, err == nil)

	resp, err := client.Do(req)
	assert.True(t, err == nil)
	assert.True(t, resp.StatusCode == http.StatusOK)
}
