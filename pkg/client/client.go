package kvs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/serviceinfo"
)

// TODO: Figure out how to pipe all the requests that modify the storage into one
//

const (
	// All the entries in this table should be unique,
	// since their values are used to assign callbacks to a command's callback table
	// otherwise hash collisions are not avoided
	cmdIntAdd    = "intadd"
	cmdIntGet    = "intget"
	cmdIntDel    = "intdel"
	cmdIntIncr   = "intincr"
	cmdIntIncrBy = "intincrby"
	cmdF32Add    = "floatadd"
	cmdF32Get    = "floatget"
	cmdF32Del    = "floatdel"
	cmdStrAdd    = "stradd"
	cmdStrGet    = "strget"
	cmdStrDel    = "strdel"
	cmdMapAdd    = "mapadd"
	cmdMapGet    = "mapget"
	cmdMapDel    = "mapdel"
	cmdUintAdd   = "uintadd"
	cmdUintGet   = "uintget"
	cmdUintDel   = "uintdel"
	cmdDel       = "del"

	cmdEcho  = "echo"
	cmdHello = "hello"
	cmdFibo  = "fibo"
	// cmdKill  = "kill" // NOTE: Kill command is not implemented yet
)

type IntCmdCallback func(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd
type StrCmdCallback func(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd
type MapCmdCallback func(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd
type BoolCmdCallback func(c *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd
type F32CmdCallback func(c *Client, ctx context.Context, cmd *FloatCmd) *FloatCmd

type Settings struct {
	Endpoint     string
	CertPemFile  string
	KeyPemFile   string
	RetriesCount int
}

type Client struct {
	*http.Client
	settings *Settings
	baseURL  *url.URL
}

type httpStatus struct {
	statusCode int
	status     string
	err        error
}

type httpResult struct {
	httpStatus
	contents []byte
}

type baseCmd struct {
	httpStatus
	args []interface{}
}

type MapCmd struct {
	baseCmd
	result map[string]string
}

type IntCmd struct {
	baseCmd
	result int
}

type StrCmd struct {
	baseCmd
	result string
}

type FloatCmd struct {
	baseCmd
	result float32
}

type BoolCmd struct {
	baseCmd
	result bool
}

type CmdTable struct {
	intCbtable  map[string]IntCmdCallback
	strCbtable  map[string]StrCmdCallback
	mapCbtable  map[string]MapCmdCallback
	f32CbTable  map[string]F32CmdCallback
	boolCbTable map[string]BoolCmdCallback
}

type Batch struct {
	queue []string // string pairs
	// key-value queue
}

func (b *Batch) Exec() {
	// make an actual http request here
}

var cmdCallbacksTable *CmdTable

func newCmdGroup() *CmdTable {
	return &CmdTable{
		intCbtable:  make(map[string]IntCmdCallback),
		strCbtable:  make(map[string]StrCmdCallback),
		mapCbtable:  make(map[string]MapCmdCallback),
		f32CbTable:  make(map[string]F32CmdCallback),
		boolCbTable: make(map[string]BoolCmdCallback),
	}
}

func init() {
	cmdCallbacksTable = newCmdGroup()

	cmdCallbacksTable.intCbtable[cmdIntGet] = intGetCommand
	cmdCallbacksTable.intCbtable[cmdIntAdd] = intAddCommand
	cmdCallbacksTable.intCbtable[cmdStrAdd] = strAddCommand
	cmdCallbacksTable.intCbtable[cmdMapAdd] = mapAddCommand
	cmdCallbacksTable.intCbtable[cmdF32Add] = f32AddCommand
	cmdCallbacksTable.intCbtable[cmdIntIncr] = intIncrCommand
	cmdCallbacksTable.intCbtable[cmdIntIncrBy] = intIncrByCommand
	// fibo rpc
	cmdCallbacksTable.intCbtable[cmdFibo] = fiboCommand

	cmdCallbacksTable.strCbtable[cmdStrGet] = strGetCommand
	cmdCallbacksTable.strCbtable[cmdEcho] = echoCommand
	cmdCallbacksTable.strCbtable[cmdHello] = helloCommand

	cmdCallbacksTable.mapCbtable[cmdMapGet] = mapGetCommand

	cmdCallbacksTable.boolCbTable[cmdIntDel] = intDelCommand
	cmdCallbacksTable.boolCbTable[cmdStrDel] = strDelCommand
	cmdCallbacksTable.boolCbTable[cmdMapDel] = mapDelCommand
	cmdCallbacksTable.boolCbTable[cmdF32Del] = f32DelCommand
	// callback for killing the server
	// cmdCallbacksTable.boolCbTable[cmdKill] = killCommand
	// del a key from any type of storage
	cmdCallbacksTable.boolCbTable[cmdDel] = delCommand

	cmdCallbacksTable.f32CbTable[cmdF32Get] = f32GetCommand
}

func NewClient(settings *Settings) *Client {
	// TODO: Change to https once we switch to TLS
	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/%s/%s/", settings.Endpoint, info.ServiceName(), info.ServiceVersion()))
	return &Client{
		settings: settings,
		Client:   &http.Client{},
		baseURL:  baseURL,
	}
}

func (c *MapCmd) Result() map[string]string {
	return c.result
}

func (c *IntCmd) Result() int {
	return c.result
}

func (c *StrCmd) Result() string {
	return c.result
}

func (c *FloatCmd) Result() float32 {
	return c.result
}

func (c *BoolCmd) Result() bool {
	return c.result
}

func (c *baseCmd) Error() error {
	return c.err
}

func (c *baseCmd) Status() string {
	return c.status
}

func (c *baseCmd) StatusCode() int {
	return c.statusCode
}

func (c *baseCmd) Args() []interface{} {
	return c.args
}

func (c *baseCmd) setResponseInfo(statusCode int, status string, err error) {
	c.statusCode = statusCode
	c.status = status
	c.err = err
}

func extendArgs(arr []interface{}, args ...interface{}) {
	for idx, arg := range args {
		arr[idx] = arg
	}
}

func newMapCmd(args ...interface{}) *MapCmd {
	return &MapCmd{baseCmd: baseCmd{args: args}}
}

func newStrCmd(args ...interface{}) *StrCmd {
	return &StrCmd{baseCmd: baseCmd{args: args}}
}

func newBoolCmd(args ...interface{}) *BoolCmd {
	return &BoolCmd{baseCmd: baseCmd{args: args}}
}

func newIntCmd(args ...interface{}) *IntCmd {
	return &IntCmd{baseCmd: baseCmd{args: args}}
}

func newFloatCmd(args ...interface{}) *FloatCmd {
	return &FloatCmd{baseCmd: baseCmd{args: args}}
}

func (c *Client) Echo(ctx context.Context, val string) *StrCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdEcho, val)
	cmd := newStrCmd(args...)
	cmdCallbacksTable.strCbtable[cmdEcho](c, ctx, cmd)
	return cmd
}

func (c *Client) Hello(ctx context.Context) *StrCmd {
	args := make([]interface{}, 1)
	args[0] = cmdHello
	cmd := newStrCmd(args...)
	cmdCallbacksTable.strCbtable[cmdHello](c, ctx, cmd)
	return cmd
}

func (c *Client) Fibo(ctx context.Context, n int) *IntCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdFibo, n)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdFibo](c, ctx, cmd)
	return cmd
}

// func (c *Client) Kill(ctx context.Context) *BoolCmd {
// 	args := make([]interface{}, 1)
// 	args[0] = cmdKill
// 	cmd := newBoolCmd(args...)
// 	cmdCallbacksTable.boolCbTable[cmdKill](c, ctx, cmd)
// 	return cmd
// }

func (c *Client) Del(ctx context.Context, key string) *BoolCmd {
	cmd := newBoolCmd(cmdDel, key)
	cmdCallbacksTable.boolCbTable[cmdDel](c, ctx, cmd)
	return cmd
}

func (c *Client) StrGet(ctx context.Context, key string) *StrCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdStrGet, key)
	cmd := newStrCmd(args...)
	cmdCallbacksTable.strCbtable[cmdStrGet](c, ctx, cmd)
	return cmd
}

func (c *Client) StrAdd(ctx context.Context, key string, val string) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdStrAdd, key, val)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdStrAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) StrDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdStrDel, key)
	cmd := newBoolCmd(args...)
	cmdCallbacksTable.boolCbTable[cmdStrDel](c, ctx, cmd)
	return cmd
}

func (c *Client) IntGet(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntGet, key)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdIntGet](c, ctx, cmd)
	return cmd
}

func (c *Client) IntAdd(ctx context.Context, key string, val int) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdIntAdd, key, val)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdIntAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) IntDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntDel, key)
	cmd := newBoolCmd(args...)
	cmdCallbacksTable.boolCbTable[cmdIntDel](c, ctx, cmd)
	return cmd
}

func (c *Client) IntIncr(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntIncr, key)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdIntIncr](c, ctx, cmd)
	return cmd
}

func (c *Client) IntIncrBy(ctx context.Context, key string, val int) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdIntIncrBy, key, val)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdIntIncrBy](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Get(ctx context.Context, key string) *FloatCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdF32Get, key)
	cmd := newFloatCmd(args...)
	cmdCallbacksTable.f32CbTable[cmdF32Get](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Add(ctx context.Context, key string, val float32) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdF32Add, key, val)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdF32Add](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Del(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdF32Del, key)
	cmd := newBoolCmd(args...)
	cmdCallbacksTable.boolCbTable[cmdF32Del](c, ctx, cmd)
	return cmd
}

func (c *Client) MapGet(ctx context.Context, key string) *MapCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdMapGet, key)
	cmd := newMapCmd(args...)
	cmdCallbacksTable.mapCbtable[cmdMapGet](c, ctx, cmd)
	return cmd
}

func (c *Client) MapAdd(ctx context.Context, key string, val map[string]string) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdMapAdd, key, val)
	cmd := newIntCmd(args...)
	cmdCallbacksTable.intCbtable[cmdMapAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) MapDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdMapDel, key)
	cmd := newBoolCmd(args...)
	cmdCallbacksTable.boolCbTable[cmdMapDel](c, ctx, cmd)
	return cmd
}

func echoCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	val := cmd.args[1].(string)
	r := client.httpRequest(ctx, http.MethodPost, url, bytes.NewBufferString(val), nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	cmd.result = string(r.contents)
	return cmd
}

func helloCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	r := client.httpRequest(ctx, http.MethodPost, url, nil, nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	cmd.result = string(r.contents)
	return cmd
}

func fiboCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	// NOTE: `n` is passed as a query string
	// http://.../service-info/fibo?n=12 for example
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	query := url.Query()
	query.Add("n", strconv.Itoa(cmd.args[1].(int)))
	url.RawQuery = query.Encode()
	log.Logger.Info("Query URL: %s", url.String())
	r := client.httpRequest(ctx, http.MethodPost, url, nil, nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	cmd.result, _ = strconv.Atoi(string(r.contents))
	return cmd
}

// func killCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
// 	url := client.baseURL.JoinPath(cmd.args[0].(string))
// 	res := performHttpRequest(client, ctx, http.MethodPost, url.String(), nil)
// 	cmd.cmdStatus = res.cmdStatus
// 	return cmd
// }

func delCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodDelete, url, nil, nil)
	_ = postDelCommand(r, cmd)
	return cmd
}

func strGetCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodGet, url, nil, nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	cmd.result = string(r.contents)
	return cmd
}

func strAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := cmd.args[2].(string)
	r := client.httpRequest(ctx, http.MethodPut, url, bytes.NewBufferString(val), nil)
	_ = postAddCommand(r, cmd)
	return cmd
}

func strDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodDelete, url, nil, nil)
	_ = postDelCommand(r, cmd)
	return cmd
}

func intGetCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodGet, url, nil, nil)
	_ = postIntGetCommand(r, cmd)
	return cmd
}

func intAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := fmt.Sprintf("%d", cmd.args[2].(int))
	r := client.httpRequest(ctx, http.MethodPut, url, bytes.NewBufferString(val), nil)
	cmd.httpStatus = r.httpStatus
	return cmd
}

func intDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodDelete, url, nil, nil)
	_ = postDelCommand(r, cmd)
	return cmd
}

func intIncrCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodPut, url, nil, nil)
	_ = postIntGetCommand(r, cmd)
	return cmd
}

func intIncrByCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := strconv.FormatInt(int64(cmd.args[2].(int)), 10)
	r := client.httpRequest(ctx, http.MethodPut, url, bytes.NewBufferString(val), nil)
	_ = postIntGetCommand(r, cmd)
	return cmd
}

func f32GetCommand(client *Client, ctx context.Context, cmd *FloatCmd) *FloatCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodGet, url, nil, nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	// We have to be careful with the precision here,
	// because some of the tests might fail.
	val, _ := strconv.ParseFloat(string(r.contents), 32)
	cmd.result = float32(val)
	return cmd
}

func f32AddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := fmt.Sprintf("%e", cmd.args[2].(float32))
	r := client.httpRequest(ctx, http.MethodPut, url, bytes.NewBufferString(val), nil)
	_ = postAddCommand(r, cmd)
	return cmd
}

func f32DelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodDelete, url, nil, nil)
	_ = postDelCommand(r, cmd)
	return cmd
}

func mapGetCommand(client *Client, ctx context.Context, cmd *MapCmd) *MapCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodGet, url, nil, nil)
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	val := make(map[string]string)
	err := json.Unmarshal(r.contents, &val)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.result = val
	return cmd
}

func mapAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	hashmap := cmd.args[2].(map[string]string)
	val, _ := json.Marshal(hashmap)
	r := client.httpRequest(ctx, http.MethodPut, url, bytes.NewBuffer(val), nil)
	_ = postAddCommand(r, cmd)
	return cmd
}

func mapDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	r := client.httpRequest(ctx, http.MethodDelete, url, nil, nil)
	_ = postDelCommand(r, cmd)
	return cmd
}

func postDelCommand(r *httpResult, cmd *BoolCmd) *BoolCmd {
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	// NOTE: If the contents doesn't equal to nil, the key was deleted,
	// otherwise it didn't exist in the storage
	if r.contents != nil {
		cmd.result = true
	}
	return cmd
}

func postAddCommand(r *httpResult, cmd *IntCmd) *IntCmd {
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	// NOTE: Assign the status code to the result
	cmd.result = r.statusCode
	return cmd
}

// TODO: Make it possible to pass the base
func postIntGetCommand(r *httpResult, cmd *IntCmd) *IntCmd {
	cmd.httpStatus = r.httpStatus
	if r.err != nil {
		return cmd
	}
	val, err := strconv.ParseInt(string(r.contents), 0, 32)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.result = int(val)
	return cmd
}

func oneOf[T string | int](src T, rest ...T) bool {
	for _, v := range rest {
		if src == v {
			return true
		}
	}
	return false
}

var retryStatusCodes = []int{
	http.StatusBadGateway,
	http.StatusTooManyRequests,
	http.StatusTooEarly,
	http.StatusGatewayTimeout,
	http.StatusRequestTimeout,
	http.StatusServiceUnavailable,
}

type _Effector func(context context.Context) (*http.Response, error)

func retry(effector _Effector, retriesMaxCount int, delay time.Duration) _Effector {
	return func(ctx context.Context) (*http.Response, error) {
		for r := 0; ; r++ {

			response, err := effector(ctx)
			if (err == nil && !oneOf(response.StatusCode, retryStatusCodes...)) || r >= retriesMaxCount {
				return response, err
			}

			if err != nil {
				log.Logger.Error("Error occured while retrying %v", err)
			}

			log.Logger.Info("Attempt %d failed; retrying in %v", r+1, delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				log.Logger.Info("Context deadline exceeded")
				return nil, ctx.Err()
			}
		}
	}
}

func (c *Client) httpRequest(ctx context.Context, httpMethod string, url *url.URL, body io.Reader, headers *map[string]string) *httpResult {
	result := &httpResult{}

	req, err := http.NewRequestWithContext(ctx, httpMethod, url.String(), body)
	if body != nil {
		switch t := body.(type) {
		case *bytes.Buffer:
			log.Logger.Info("Setting request headers, body: %s", t.String())

			req.Header.Add("content-type", "application/octet-stream")
			req.Header.Add("content-length", fmt.Sprintf("%d", t.Len()))
		} // TODO: Handle unknown type?
	}

	effector := func(ctx context.Context) (*http.Response, error) {
		if headers != nil {
			for key, value := range *headers {
				req.Header.Add(key, value)
			}
		}

		if err != nil {
			return nil, err
		}

		// The request Body, if non-nil, will be closed by the underlying Transport, even on errors.
		// The Body may be closed asynchronously after Do returns.
		return c.Do(req)
	}

	closure := retry(effector, c.settings.RetriesCount, 2000*time.Millisecond)
	r, err := closure(ctx)
	if err != nil {
		result.err = err
		return result
	}
	defer r.Body.Close()

	result.status = r.Status
	result.statusCode = r.StatusCode

	data, _ := io.ReadAll(r.Body)
	if !oneOf[int](r.StatusCode, http.StatusOK, http.StatusCreated, http.StatusAccepted) {
		// NOTE: If the response status doesn't equal to 200, 201 or 202, the body will contain an error message
		result.err = errors.New(string(data))
		return result
	}

	result.contents = data
	return result
}
