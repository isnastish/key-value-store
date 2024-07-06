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
	cmdKill  = "kill"
	cmdFibo  = "fibo"
)

type IntCmdCallback func(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd
type StrCmdCallback func(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd
type MapCmdCallback func(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd
type BoolCmdCallback func(c *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd
type F32CmdCallback func(c *Client, ctx context.Context, cmd *FloatCmd) *FloatCmd

type Settings struct {
	Endpoint    string
	CertPemFile string
	KeyPemFile  string
	RetryCount  int
}

type Client struct {
	*http.Client
	settings *Settings
	baseURL  *url.URL
}

type cmdStatus struct {
	statusCode int
	status     string
	err        error
}
type baseCmd struct {
	cmdStatus
	args []interface{}
}

type Result struct {
	cmdStatus
	contents []byte
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
	cmdCallbacksTable.boolCbTable[cmdKill] = killCommand
	// del a key from any type of storage
	cmdCallbacksTable.boolCbTable[cmdDel] = delCommand

	cmdCallbacksTable.f32CbTable[cmdF32Get] = f32GetCommand
}

func NewClient(settings *Settings) *Client {
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

func (c *Client) Kill(ctx context.Context) *BoolCmd {
	args := make([]interface{}, 1)
	args[0] = cmdKill
	cmd := newBoolCmd(args...)
	cmdCallbacksTable.boolCbTable[cmdKill](c, ctx, cmd)
	return cmd
}

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
	res := performHttpRequest(client, ctx, http.MethodPost, url.String(), bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if cmd.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func helloCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	res := performHttpRequest(client, ctx, http.MethodPost, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if cmd.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func fiboCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	query := url.Query()
	query.Add("n", strconv.Itoa(cmd.args[1].(int)))
	url.RawQuery = query.Encode()
	res := performHttpRequest(client, ctx, http.MethodPost, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if cmd.err != nil {
		return cmd
	}
	val, _ := strconv.Atoi(string(res.contents))
	cmd.result = val
	return cmd
}

func killCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string))
	res := performHttpRequest(client, ctx, http.MethodPost, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	return cmd
}

func delCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodDelete, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if cmd.err != nil {
		return cmd
	}
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func strGetCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodGet, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func strAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := cmd.args[2].(string)
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// I cannot think of anything else rather than putting a status
	// from a PUT operation into a result itself.
	cmd.result = res.statusCode
	return cmd
}

func strDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodDelete, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// If result's contents is not empty the value was deleted, so we set result to true,
	// false otherwise if DELETE had no effect.
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func intGetCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodGet, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result, _ = strconv.Atoi(string(res.contents))
	return cmd
}

func intAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := fmt.Sprintf("%d", cmd.args[2].(int))
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	return cmd
}

func intDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodDelete, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// If the contents is not empty, the value was present before deletion,
	// and was successfully removed. If it's empty, then the value was't present and the deletion operatation had no effect.
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func intIncrCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	val, err := strconv.ParseInt(string(res.contents), 0, 32)
	if err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = int(val)
	return cmd
}

func intIncrByCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := strconv.FormatInt(int64(cmd.args[2].(int)), 10)
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	resVal, err := strconv.ParseInt(string(res.contents), 0, 32)
	if err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = int(resVal)
	return cmd
}

func f32GetCommand(client *Client, ctx context.Context, cmd *FloatCmd) *FloatCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodGet, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// We have to be careful with the precision here,
	// because some of the tests might fail.
	val, _ := strconv.ParseFloat(string(res.contents), 32)
	cmd.result = float32(val)
	return cmd
}

func f32AddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	val := fmt.Sprintf("%e", cmd.args[2].(float32))
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// Again, put the status code as a result of an operation
	cmd.result = res.statusCode
	return cmd
}

func f32DelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodDelete, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func mapGetCommand(client *Client, ctx context.Context, cmd *MapCmd) *MapCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodGet, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	val := make(map[string]string)
	json.Unmarshal(res.contents, &val)
	cmd.result = val
	return cmd
}

func mapAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	hashmap := cmd.args[2].(map[string]string)
	val, _ := json.Marshal(hashmap)
	res := performHttpRequest(client, ctx, http.MethodPut, url.String(), bytes.NewBuffer(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = res.statusCode
	return cmd
}

func mapDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	url := client.baseURL.JoinPath(cmd.args[0].(string)).JoinPath(cmd.args[1].(string))
	res := performHttpRequest(client, ctx, http.MethodDelete, url.String(), nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	if res.contents != nil {
		cmd.result = true
	}
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

type cancellableTimer struct {
	cancelCh chan bool
}

func newCancellableTimer() *cancellableTimer {
	return &cancellableTimer{
		cancelCh: make(chan bool),
	}
}

func (t *cancellableTimer) waitWithCancel(duration time.Duration, result chan<- bool) {
	select {
	case <-time.After(duration):
		result <- true
	case <-t.cancelCh:
		result <- false
	}
}

func (t *cancellableTimer) cancel() {
	close(t.cancelCh)
}

func doRequestWithRetry(client *Client, ctx context.Context, retriesCount int, duration time.Duration, req *http.Request) (*http.Response, error) {
	ctimer := newCancellableTimer()
	responseCh := make(chan *http.Response, 1)
	errorCh := make(chan error, 1)

	go func() {
		for retries := 0; retries < retriesCount; retries++ {
			resp, err := client.Do(req)
			if err != nil {
				errorCh <- err
				return
			}

			if oneOf[int](resp.StatusCode,
				http.StatusBadGateway,
				http.StatusTooManyRequests,
				http.StatusTooEarly,
				http.StatusGatewayTimeout,
				http.StatusRequestTimeout,
				http.StatusServiceUnavailable) {

				log.Logger.Info("Attempt %d failed with status %s, retrying in %v",
					retries+1, resp.Status, duration,
				)

			} else {
				responseCh <- resp
				return
			}

			// We cancel the timer when context deadline signal is received.
			resultCh := make(chan bool)
			go ctimer.waitWithCancel(duration, resultCh)
			if res := <-resultCh; !res {
				return
			}
		}

		errorCh <- errors.New("Retries limit exceeded")
	}()

	select {
	case err := <-errorCh:
		return nil, err
	case resp := <-responseCh:
		return resp, nil
	case <-ctx.Done():
		ctimer.cancel()
		return nil, ctx.Err()
	}
}

// https://stackoverflow.com/questions/65950011/what-is-the-best-practice-when-using-with-context-withtimeout-in-go
// TODO: Handle an error when a server hasn't started yet.
// Because closing the response body will most likely block.
func performHttpRequest(client *Client, ctx context.Context, httpMethod string, url string, body io.Reader) *Result {
	var req *http.Request
	var err error
	var result = &Result{}
	var stream []byte

	req, err = http.NewRequestWithContext(ctx, httpMethod, url, body)
	if err != nil {
		result.err = err
		return result
	}

	if httpMethod == http.MethodPut && body != nil {
		switch t := body.(type) {
		case *bytes.Buffer:
			req.Header.Add("Content-Length", fmt.Sprintf("%d", t.Len()))
			req.Header.Add("Content-Type", "application/octet-stream")
		}
	}

	req.Header.Add("User-Agent", "kvs-client")

	resp, err := doRequestWithRetry(client, ctx, client.settings.RetryCount, 2000*time.Millisecond, req)
	if err != nil {
		result.err = err
		return result
	}

	defer resp.Body.Close()

	result.status = resp.Status
	result.statusCode = resp.StatusCode

	if resp.StatusCode != http.StatusOK {
		// If the response status doesn't equal to 200, the body will contain an error message
		// prescribed by the service.
		stream, _ = io.ReadAll(resp.Body)
		result.err = errors.New(string(stream))
		return result
	}

	if oneOf[string](httpMethod, http.MethodGet, http.MethodPost, http.MethodPut) {
		stream, _ = io.ReadAll(resp.Body)
		return result
	}

	// On a successfull DELETE request, the response MAY contain a Deleted header,
	// which is used to identify whether a resource was deleted or not.
	if httpMethod == http.MethodDelete {
		result.contents = []byte(resp.Header.Get("Deleted"))
		return result
	}

	return result
}
