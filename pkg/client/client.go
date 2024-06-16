package kvs

import (
	"fmt"
	"io"
	"net/http"
	_ "net/url"
)

type Settings struct {
	KvsEndpoint  string
	RetriesCount uint
}

type baseResultCmd struct {
	err error
}

type ResultCmd struct {
	baseResultCmd

	val string
}

type client struct {
	settings   *Settings
	httpClient *http.Client
}

func (r *baseResultCmd) Error() error {
	return r.err
}

func (r *ResultCmd) Value() string {
	return r.val
}

func NewResultCmd(err error, args ...string) *ResultCmd {
	var val string
	if len(args) > 0 {
		val = args[0]
	}

	return &ResultCmd{
		baseResultCmd: baseResultCmd{
			err: err,
		},
		val: val,
	}
}

func Client(settings *Settings) *client {
	return &client{
		settings:   settings,
		httpClient: &http.Client{},
	}
}

func (c *client) Hello() *ResultCmd {
	resp, err := c.httpClient.Get(fmt.Sprintf("http://%s/v1/echo", c.settings.KvsEndpoint))
	if err != nil {
		return NewResultCmd(fmt.Errorf("failed to make GET request %v", err))
	}

	if resp.StatusCode != http.StatusOK {
		return NewResultCmd(fmt.Errorf("status code %s", resp.Status))
	}

	defer resp.Body.Close()
	result, err := io.ReadAll(resp.Body)
	if err != nil {
		return NewResultCmd(fmt.Errorf("failed to read from response body %v", err))
	}
	return NewResultCmd(nil, string(result))
}
