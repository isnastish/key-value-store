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

	"github.com/isnastish/kvs/pkg/version"
)

// TODO: Add retries to makeHttpRequest function on specific error codes.

type cmd string

const (
	// All the entries in this table should be unique,
	// since their values are used to assign callbacks to a command's callback table
	// otherwise hash collisions are not avoided
	cmdIntAdd    = cmd("intadd")
	cmdIntGet    = cmd("intget")
	cmdIntDel    = cmd("intdel")
	cmdIntIncr   = cmd("intincr")
	cmdIntIncrBy = cmd("intincrby")
	cmdF32Add    = cmd("floatadd")
	cmdF32Get    = cmd("floatget")
	cmdF32Del    = cmd("floatdel")
	cmdStrAdd    = cmd("stradd")
	cmdStrGet    = cmd("strget")
	cmdStrDel    = cmd("strdel")
	cmdMapAdd    = cmd("mapadd")
	cmdMapGet    = cmd("mapget")
	cmdMapDel    = cmd("mapdel")
	cmdUintAdd   = cmd("uintadd")
	cmdUintGet   = cmd("uintget")
	cmdUintDel   = cmd("uintdel")

	cmdEcho  = cmd("echo")
	cmdHello = cmd("hello")
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
	RetriesCount uint
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

type reqResult struct {
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

type CmdGroup struct {
	intCbtable  map[cmd]IntCmdCallback
	strCbtable  map[cmd]StrCmdCallback
	mapCbtable  map[cmd]MapCmdCallback
	f32CbTable  map[cmd]F32CmdCallback
	boolCbTable map[cmd]BoolCmdCallback
}

var cmdCallbacksTable *CmdGroup

func newCmdGroup() *CmdGroup {
	return &CmdGroup{
		intCbtable:  make(map[cmd]IntCmdCallback),
		strCbtable:  make(map[cmd]StrCmdCallback),
		mapCbtable:  make(map[cmd]MapCmdCallback),
		f32CbTable:  make(map[cmd]F32CmdCallback),
		boolCbTable: make(map[cmd]BoolCmdCallback),
	}
}

func init() {
	cmdCallbacksTable = newCmdGroup()

	cmdCallbacksTable.intCbtable[cmdIntGet] = intGetCommand
	cmdCallbacksTable.intCbtable[cmdIntGet] = intAddCommand
	cmdCallbacksTable.intCbtable[cmdStrAdd] = strAddCommand
	cmdCallbacksTable.intCbtable[cmdMapAdd] = mapAddCommand
	cmdCallbacksTable.intCbtable[cmdF32Add] = f32AddCommand
	cmdCallbacksTable.intCbtable[cmdIntIncr] = intIncrCommand
	cmdCallbacksTable.intCbtable[cmdIntIncrBy] = intIncrByCommand

	cmdCallbacksTable.strCbtable[cmdStrGet] = strGetCommand
	cmdCallbacksTable.strCbtable[cmdEcho] = echoCommand
	cmdCallbacksTable.strCbtable[cmdHello] = helloCommand

	cmdCallbacksTable.mapCbtable[cmdMapGet] = mapGetCommand

	cmdCallbacksTable.boolCbTable[cmdIntDel] = intDelCommand
	cmdCallbacksTable.boolCbTable[cmdStrDel] = strDelCommand
	cmdCallbacksTable.boolCbTable[cmdMapDel] = mapDelCommand
	cmdCallbacksTable.boolCbTable[cmdF32Del] = f32DelCommand

	cmdCallbacksTable.f32CbTable[cmdF32Get] = f32GetCommand
}

func NewClient(settings *Settings) *Client {
	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/api/%s/", settings.Endpoint, version.GetServiceVersion()))
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
	_ = cmdCallbacksTable.strCbtable[cmdEcho](c, ctx, cmd)
	return cmd
}

func (c *Client) Hello(ctx context.Context) *StrCmd {
	args := make([]interface{}, 1)
	args[0] = cmdHello
	cmd := newStrCmd(args...)
	_ = cmdCallbacksTable.strCbtable[cmdHello](c, ctx, cmd)
	return cmd
}

func (c *Client) StrGet(ctx context.Context, key string) *StrCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdStrGet, key)
	cmd := newStrCmd(args...)
	_ = cmdCallbacksTable.strCbtable[cmdStrGet](c, ctx, cmd)
	return cmd
}

func (c *Client) StrAdd(ctx context.Context, key string, val string) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdStrAdd, key, val)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdStrAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) StrDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdStrDel, key)
	cmd := newBoolCmd(args...)
	_ = cmdCallbacksTable.boolCbTable[cmdStrDel](c, ctx, cmd)
	return cmd
}

func (c *Client) IntGet(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntGet, key)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdIntGet](c, ctx, cmd)
	return cmd
}

func (c *Client) IntAdd(ctx context.Context, key string, val int) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdIntAdd, key, val)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdIntAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) IntDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntDel, key)
	cmd := newBoolCmd(args...)
	_ = cmdCallbacksTable.boolCbTable[cmdIntDel](c, ctx, cmd)
	return cmd
}

func (c *Client) IntIncr(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdIntIncr, key)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdIntIncr](c, ctx, cmd)
	return cmd
}

func (c *Client) IntIncrBy(ctx context.Context, key string, val int) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdIntIncrBy, key, val)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdIntIncrBy](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Get(ctx context.Context, key string) *FloatCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdF32Get, key)
	cmd := newFloatCmd(args...)
	_ = cmdCallbacksTable.f32CbTable[cmdF32Get](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Add(ctx context.Context, key string, val float32) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdF32Add, key, val)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdF32Add](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Del(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdF32Del, key)
	cmd := newBoolCmd(args...)
	_ = cmdCallbacksTable.boolCbTable[cmdF32Del](c, ctx, cmd)
	return cmd
}

func (c *Client) MapGet(ctx context.Context, key string) *MapCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdMapGet, key)
	cmd := newMapCmd(args...)
	_ = cmdCallbacksTable.mapCbtable[cmdMapGet](c, ctx, cmd)
	return cmd
}

func (c *Client) MapAdd(ctx context.Context, key string, val map[string]string) *IntCmd {
	args := make([]interface{}, 3)
	extendArgs(args, cmdMapAdd, key, val)
	cmd := newIntCmd(args...)
	_ = cmdCallbacksTable.intCbtable[cmdMapAdd](c, ctx, cmd)
	return cmd
}

func (c *Client) MapDel(ctx context.Context, key string) *BoolCmd {
	args := make([]interface{}, 2)
	extendArgs(args, cmdMapDel, key)
	cmd := newBoolCmd(args...)
	_ = cmdCallbacksTable.boolCbTable[cmdMapDel](c, ctx, cmd)
	return cmd
}

func helloCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func echoCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string)
	val := cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func strGetCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func strAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := cmd.args[2].(string)
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodDelete, path, nil)
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result, _ = strconv.Atoi(string(res.contents))
	return cmd
}

func intAddCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := fmt.Sprintf("%d", cmd.args[2].(int))
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	return cmd
}

func intDelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodDelete, path, nil)
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodPut, path, nil)
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := strconv.FormatInt(cmd.args[2].(int64), 10)
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
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
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := fmt.Sprintf("%e", cmd.args[2].(float32))
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	// Again, put the status code as a result of an operation
	cmd.result = res.statusCode
	return cmd
}

func f32DelCommand(client *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodDelete, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func mapGetCommand(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(c, ctx, http.MethodGet, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	val := make(map[string]string)
	json.Unmarshal(res.contents, &val) // shouldn't fail if was properly marshalled on the server side
	cmd.result = val
	return cmd
}

func mapAddCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	hashmap := cmd.args[2].(map[string]string)
	val, _ := json.Marshal(hashmap)
	res := makeHttpRequest(c, ctx, http.MethodPut, path, bytes.NewBuffer(val))
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	cmd.result = res.statusCode
	return cmd
}

func mapDelCommand(c *Client, ctx context.Context, cmd *BoolCmd) *BoolCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(c, ctx, http.MethodDelete, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	if res.contents != nil {
		cmd.result = true
	}
	return cmd
}

func incrCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	panic("Not implemented!")
}

func incrByCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	panic("Not implemented!")
}

func makeHttpRequest(client *Client, ctx context.Context, httpMethod string, path string, contents *bytes.Buffer) *reqResult {
	result := &reqResult{}
	URL := client.baseURL.JoinPath(path)
	var req *http.Request
	var err error
	// NewRequestWithContext checks the type of the body, if the type is bytes.Buffer,
	// and the buffer is nil, it will try to dereference a nil pointer.
	// This is a quick and hacky way how to prevent that.
	// Should investigate this problem more in depth.
	// Go standard library checks whether body is equal to nil, so I have to investigte this problem more.
	if contents == nil {
		req, err = http.NewRequestWithContext(ctx, httpMethod, URL.String(), nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, httpMethod, URL.String(), contents)
	}
	if err != nil {
		result.err = err
		return result
	}
	if httpMethod == http.MethodPut {
		req.Header.Add("Content-Length", fmt.Sprintf("%d", contents.Len()))
		req.Header.Add("Content-Type", "text/plain")
	}
	req.Header.Add("User-Agent", "kvs-client")
	// Non 2xx status code doesn't cause an error
	resp, err := client.Do(req)
	if err != nil {
		result.err = err
		return result
	}

	result.status = resp.Status
	result.statusCode = resp.StatusCode

	// If the response status for the GET request doesn't equal to 200,
	// we need to convert the response status into an error.
	// In this situation, the body will contain an error message assigned by the server.
	if (httpMethod == http.MethodGet && resp.StatusCode != http.StatusOK) ||
		(httpMethod == http.MethodPut && resp.StatusCode != http.StatusCreated) ||
		(httpMethod == http.MethodDelete && resp.StatusCode != http.StatusNoContent) {
		bytes, _ := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		result.err = errors.New(string(bytes))
		return result
	}

	// If true is returned, the value was deleted.
	if httpMethod == http.MethodDelete {
		result.contents = []byte(resp.Header.Get("Deleted"))
	}

	// Parse the body for all methods? We will need them for Incr/IncrBy methods
	if httpMethod == http.MethodGet || httpMethod == http.MethodPut {
		bytes, _ := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		result.contents = bytes
	}

	return result
}
