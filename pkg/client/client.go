package kvs

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

type IntCommandCallback func(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd
type StringCommandCallback func(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd
type MapCommandCallback func(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd

type Settings struct {
	Endpoint     string
	RetriesCount uint
	TlsConfig    *tls.Config
}

type Client struct {
	*http.Client
	settings *Settings
	baseURL  *url.URL
}

type baseCmd struct {
	args []interface{}
	err  error
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

// BoolCmd will be used for sets to check whether they contain members,
// maybe even for maps as well.
type BoolCmd struct {
	baseCmd
	result bool
}

type CmdGroup struct {
	intCbtable map[string]IntCommandCallback
	strCbtable map[string]StringCommandCallback
	mapCbtable map[string]MapCommandCallback
}

var cmdgroup *CmdGroup

func newCmdGroup() *CmdGroup {
	return &CmdGroup{
		intCbtable: make(map[string]IntCommandCallback),
		strCbtable: make(map[string]StringCommandCallback),
		mapCbtable: make(map[string]MapCommandCallback),
	}
}

func init() {
	cmdgroup = newCmdGroup()

	cmdgroup.intCbtable["intget"] = intGetCommand
	cmdgroup.intCbtable["intput"] = intPutCommand
	cmdgroup.intCbtable["intdel"] = intDelCommand
	cmdgroup.intCbtable["strput"] = stringPutCommand
	cmdgroup.intCbtable["strdel"] = stringDelCommand
	cmdgroup.intCbtable["mapdel"] = mapDelCommand
	cmdgroup.intCbtable["mapput"] = mapPutCommand

	cmdgroup.strCbtable["strget"] = stringGetCommand
	cmdgroup.strCbtable["echo"] = echoCommand
	cmdgroup.strCbtable["hello"] = helloCommand

	cmdgroup.mapCbtable["mapget"] = mapGetCommand
}

func NewClient(settings *Settings) *Client {
	baseURL, _ := url.Parse(fmt.Sprintf("http://%s/", settings.Endpoint))
	return &Client{
		settings: settings,
		Client:   &http.Client{},
		baseURL:  baseURL,
	}
}

func (b *baseCmd) Err() error {
	return b.err
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

	cmd := newStrCmd()
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

func (c *Client) StrDel(ctx context.Context, key string) *IntCmd {
	cmdname := "strdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)

	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)

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

func (c *Client) IntDel(ctx context.Context, key string) *IntCmd {
	cmdname := "intdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)

	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)

	return cmd
}

func (c *Client) MapGet(ctx context.Context, key string, val map[string]string) *MapCmd {
	cmdname := "mapget"
	args := make([]interface{}, 3)
	extendArgs(args, cmdname, key, val)

	cmd := newMapCmd(args...)
	_ = cmdgroup.mapCbtable[cmdname](c, ctx, cmd)

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

func (c *Client) MapDel(ctx context.Context, key string) *IntCmd {
	cmdname := "mapdel"
	args := make([]interface{}, 2)
	extendArgs(args, cmdname, key)

	cmd := newIntCmd(args...)
	_ = cmdgroup.intCbtable[cmdname](c, ctx, cmd)

	return cmd
}

func helloCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func echoCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string)
	val := cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func stringGetCommand(client *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = string(res.contents)
	return cmd
}

func stringPutCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := cmd.args[2].(string)
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = res.code
	return cmd
}

func stringDelCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodDelete, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = res.code
	return cmd
}

func intGetCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodGet, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result, _ = strconv.Atoi(string(res.contents))
	return cmd
}

func intPutCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	val := fmt.Sprintf("%d", cmd.args[2].(int))
	res := makeHttpRequest(client, ctx, http.MethodPut, path, bytes.NewBufferString(val))
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	// Maybe result shouldn't contain the status code, at least for an integer,
	// because It will be hard to distinguish whether it's a real value or a response status
	cmd.result = res.code
	return cmd
}

func intDelCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(client, ctx, http.MethodDelete, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = res.code
	return cmd
}

func incrCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	panic("Not implemented!")
	return cmd
}

func incrByCommand(client *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	panic("Not implemented!")
	return cmd
}

func mapGetCommand(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	result := makeHttpRequest(c, ctx, http.MethodGet, path, nil)
	if result.err != nil {
		cmd.err = result.err
		return cmd
	}
	val := make(map[string]string)
	err := json.Unmarshal(result.contents, &val)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.result = val
	return cmd
}

func mapPutCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	hashmap := cmd.args[2].(map[string]string)
	val, err := json.Marshal(hashmap)
	res := makeHttpRequest(c, ctx, http.MethodPut, path, bytes.NewBuffer(val))
	if res.err != nil {
		cmd.err = err
		return cmd
	}
	cmd.result = res.code
	return cmd
}

func mapDelCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	path := cmd.args[0].(string) + "/" + cmd.args[1].(string)
	res := makeHttpRequest(c, ctx, http.MethodDelete, path, nil)
	if res.err != nil {
		cmd.err = res.err
		return cmd
	}
	cmd.result = res.code
	return cmd
}

type makeReqResult struct {
	status   string
	code     int
	contents []byte
	err      error
}

// PUT | GET | DELETE
func makeHttpRequest(client *Client, ctx context.Context, httpMethod string, path string, contents *bytes.Buffer) *makeReqResult {
	result := &makeReqResult{}
	URL := client.baseURL.JoinPath(path)
	req, err := http.NewRequestWithContext(ctx, httpMethod, URL.String(), contents)
	if err != nil {
		result.err = err
		return result
	}
	if httpMethod == http.MethodPut {
		req.Header.Add("Content-Length", fmt.Sprintf("%d", contents.Len()))
		req.Header.Add("Content-Type", "text/plain") // should be application/json for a map
	}
	req.Header.Add("User-Agent", "kvs-client")
	resp, err := client.Do(req)
	if err != nil {
		result.err = err
		return result
	}
	if httpMethod == http.MethodGet {
		bytes, err := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		result.err = err
		result.contents = bytes
	}
	result.status = resp.Status
	if httpMethod == http.MethodDelete {
		if resp.Header.Get("Deleted") != "" {
			result.code = 1
		}
	} else {
		result.code = resp.StatusCode
	}
	return result
}
