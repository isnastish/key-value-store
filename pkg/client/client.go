package kvs

// TODO: Add retries to makeHttpRequest function on specific error codes.

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
	intCbtable  map[string]IntCmdCallback
	strCbtable  map[string]StrCmdCallback
	mapCbtable  map[string]MapCmdCallback
	f32CbTable  map[string]F32CmdCallback
	boolCbTable map[string]BoolCmdCallback
}

var cmdgroup *CmdGroup

func newCmdGroup() *CmdGroup {
	return &CmdGroup{
		intCbtable:  make(map[string]IntCmdCallback),
		strCbtable:  make(map[string]StrCmdCallback),
		mapCbtable:  make(map[string]MapCmdCallback),
		f32CbTable:  make(map[string]F32CmdCallback),
		boolCbTable: make(map[string]BoolCmdCallback),
	}
}

func init() {
	cmdgroup = newCmdGroup()

	cmdgroup.intCbtable["intget"] = intGetCommand
	cmdgroup.intCbtable["intput"] = intPutCommand
	cmdgroup.intCbtable["strput"] = strPutCommand
	cmdgroup.intCbtable["mapput"] = mapPutCommand
	cmdgroup.intCbtable["floatput"] = f32PutCommand

	cmdgroup.strCbtable["strget"] = strGetCommand
	cmdgroup.strCbtable["echo"] = echoCommand
	cmdgroup.strCbtable["hello"] = helloCommand

	cmdgroup.mapCbtable["mapget"] = mapGetCommand

	cmdgroup.boolCbTable["intdel"] = intDelCommand
	cmdgroup.boolCbTable["strdel"] = strDelCommand
	cmdgroup.boolCbTable["mapdel"] = mapDelCommand
	cmdgroup.boolCbTable["floatdel"] = f32DelCommand

	cmdgroup.f32CbTable["floatget"] = f32GetCommand
}

func NewClient(settings *Settings) *Client {
	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/api/v1/", settings.Endpoint))
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
	cmdname := "echo"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, val)
	cmd := newStrCmd(args...)
	_ = cmdgroup.strCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) Hello(ctx context.Context) *StrCmd {
	cmdname := "hello"
	args := make([]interface{}, 1)
	args[0] = cmdname
	cmd := newStrCmd(args...)
	_ = cmdgroup.strCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) StrGet(ctx context.Context, key string) *StrCmd {
	cmdname := "strget"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newStrCmd(args...)
	_ = cmdgroup.strCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) StrPut(ctx context.Context, key string, val string) *IntCmd {
	cmdname := "strput"
	args := make([]interface{}, 3)
	extendArgs(args, cmdname, key, val)
	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) StrDel(ctx context.Context, key string) *BoolCmd {
	cmdname := "strdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newBoolCmd(args...)
	_ = cmdgroup.boolCbTable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) IntGet(ctx context.Context, key string) *IntCmd {
	cmdname := "intget"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) IntPut(ctx context.Context, key string, val int) *IntCmd {
	cmdname := "intput"
	args := make([]interface{}, 3)
	extendArgs(args, cmdname, key, val)
	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) IntDel(ctx context.Context, key string) *BoolCmd {
	cmdname := "intdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newBoolCmd(args...)
	_ = cmdgroup.boolCbTable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Get(ctx context.Context, key string) *FloatCmd {
	cmdName := "floatget"
	args := make([]interface{}, 2)
	extendArgs(args, cmdName, key)
	cmd := newFloatCmd(args...)
	_ = cmdgroup.f32CbTable[cmdName](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Put(ctx context.Context, key string, val float32) *IntCmd {
	cmdName := "floatput"
	args := make([]interface{}, 3)
	extendArgs(args, cmdName, key, val)
	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdName](c, ctx, cmd)
	return cmd
}

func (c *Client) F32Del(ctx context.Context, key string) *BoolCmd {
	cmdName := "floatdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdName, key)
	cmd := newBoolCmd(args...)
	_ = cmdgroup.boolCbTable[cmdName](c, ctx, cmd)
	return cmd
}

func (c *Client) MapGet(ctx context.Context, key string, val map[string]string) *MapCmd {
	cmdName := "mapget"
	args := make([]interface{}, 3)
	extendArgs(args, cmdName, key, val)
	cmd := newMapCmd(args...)
	_ = cmdgroup.mapCbtable[cmdName](c, ctx, cmd)
	return cmd
}

func (c *Client) MapPut(ctx context.Context, key string) *IntCmd {
	cmdname := "mapput"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)
	return cmd
}

func (c *Client) MapDel(ctx context.Context, key string) *BoolCmd {
	cmdname := "mapdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)
	cmd := newBoolCmd(args...)
	_ = cmdgroup.boolCbTable[cmdname](c, ctx, cmd)
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

func strPutCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
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

func intPutCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
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

func f32GetCommand(client *Client, ctx context.Context, cmd *FloatCmd) *FloatCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	cmd.cmdStatus = res.cmdStatus
	if res.err != nil {
		return cmd
	}
	val, _ := strconv.ParseFloat(string(res.contents), 32)
	cmd.result = float32(val)
	return cmd
}

func f32PutCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
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

func mapPutCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
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

	if httpMethod == http.MethodGet {
		bytes, _ := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		result.contents = bytes
	}

	return result
}
