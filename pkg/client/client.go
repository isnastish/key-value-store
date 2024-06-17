package kvs

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Settings struct {
	KvsEndpoint  string
	RetriesCount uint
	TlsConfig    *tls.Config
}

type client struct {
	settings   *Settings
	httpClient *http.Client
}

type baseResult struct {
	params []interface{}
	err    error
}

type StrResult struct {
	baseResult
	val string
}

type IntResult struct {
	baseResult
	val int
}

type StatusResult struct {
	baseResult
	code   int
	status string
}

func NewStatusResult(err error, code int, status string, cmdParams ...interface{}) *StatusResult {
	return &StatusResult{
		baseResult: baseResult{
			params: cmdParams,
			err:    err,
		},
		code:   code,
		status: status,
	}
}

func NewStrResult(err error, val string, cmdParams ...interface{}) *StrResult {
	return &StrResult{
		baseResult: baseResult{
			params: cmdParams,
			err:    err,
		},
		val: val,
	}
}

func NewIntResult(err error, val int, cmdParams ...interface{}) *IntResult {
	return &IntResult{
		baseResult: baseResult{
			params: cmdParams,
			err:    err,
		},
		val: val,
	}
}

func Client(settings *Settings) *client {
	return &client{
		settings: settings,
		httpClient: &http.Client{
			Timeout: time.Second * 10,
		},
	}
}

func (b *baseResult) Err() error {
	return b.err
}

func (s *StrResult) Val() string {
	return s.val
}

func (c *client) Echo(ctx context.Context, val string) *StrResult {
	params := make([]interface{}, 2)
	params[0] = "Echo"
	params[1] = val

	url, err := url.Parse(fmt.Sprintf("http://%s/v1/echo", c.settings.KvsEndpoint))
	if err != nil {
		return NewStrResult(err, "", params...)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url.String(), bytes.NewBufferString(val))
	if err != nil {
		return NewStrResult(err, "", params...)
	}

	req.Header.Set("User-Agent", "kvs-client")
	req.Header.Set("Content-Length", strconv.Itoa((len(val))))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return NewStrResult(err, "", params...)
	}

	if resp.StatusCode == http.StatusInternalServerError {
		return NewStrResult(fmt.Errorf("%s %d", resp.Status, resp.StatusCode), "", params...)
	}

	// TODO: Handle response code
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return NewStrResult(err, "", params...)
	}

	return NewStrResult(nil, string(body), params...)
}

func (c *client) Put(ctx context.Context, key string, val string) *IntResult {
	params := make([]interface{}, 3)
	params[0] = "Put"
	params[1] = key
	params[2] = val

	url, err := url.Parse(fmt.Sprintf("http://%s/v1/%s", c.settings.KvsEndpoint, key))
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url.String(), bytes.NewBufferString(val))
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	return NewIntResult(nil, resp.StatusCode, params...)
}

func (c *client) Del(ctx context.Context, key string) *IntResult {
	params := make([]interface{}, 2)
	params[0] = "Del"
	params[1] = key

	url, err := url.Parse(fmt.Sprintf("http://%s/v1/%s", c.settings.KvsEndpoint, key))
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	req, err := http.NewRequestWithContext(ctx, "DELETE", url.String(), nil)
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return NewIntResult(err, -1, params...)
	}

	return NewIntResult(nil, resp.StatusCode, params...)
}

func (c *client) SPut(ctx context.Context, key string, val string) *StatusResult {
	return nil
}

func (c *client) SGet(ctx context.Context, key string) *StrResult {
	return nil
}

func (c *client) SDel(ctx context.Context, key string) *StatusResult {
	return nil
}

// TODO: Return map result
func (c *client) HMPut(ctx context.Context, hash string, m map[string]string) {

}

func (c *client) HMGet(ctx context.Context, hash string) *StatusResult {
	return nil
}

func (c *client) HMDel(ctx context.Context, hash string) *StatusResult {
	return nil
}

func (c *client) SlPut(ctx context.Context, hash string, args ...string) {

}

func (c *client) SlGet(ctx context.Context, key string) {

}

func (c *client) SlDel(ctx context.Context, key string) {

}

func (c *client) IPut(ctx context.Context, key string) *StatusResult {
	return nil
}

func (c *client) IGet(ctx context.Context, key string) *IntResult {
	return nil
}

func (c *client) IDel(ctx context.Context, key string) *StatusResult {
	return nil
}

func (c *client) Incr(ctx context.Context, key string) *IntResult {
	return nil
}

func (c *client) IncrBy(ctx context.Context, key string, val int) *IntResult {
	return nil
}

// client.FPut()
// client.FGet()
// client.FDel()
