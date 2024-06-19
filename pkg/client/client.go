package kvs

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	_ "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	settings   *Settings
	httpClient *http.Client
	baseURL    *url.URL
}

type baseCmd struct {
	name string
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
		settings:   settings,
		httpClient: &http.Client{},
		baseURL:    baseURL,
	}
}

func newMapCmd(name string, args ...interface{}) *MapCmd {
	return &MapCmd{baseCmd: baseCmd{name: name, args: args}}
}

func newStrCmd(name string, args ...interface{}) *StrCmd {
	return &StrCmd{baseCmd: baseCmd{name: name, args: args}}
}

func newBoolCmd(name string, args ...interface{}) *BoolCmd {
	return &BoolCmd{baseCmd: baseCmd{name: name, args: args}}
}

func newIntCmd(name string, args ...interface{}) *IntCmd {
	return &IntCmd{baseCmd: baseCmd{name: name, args: args}}
}

func (c *Client) Echo(ctx context.Context, val string) *StrCmd {
	args := make([]interface{}, 1)
	args[1] = val

	cmd := newStrCmd("echo", args)
	_ = cmdgroup.strCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) Hello(ctx context.Context) *StrCmd {
	cmd := newStrCmd("hello")
	_ = cmdgroup.strCbtable[cmd.name](c, ctx, cmd)
	return cmd
}

func (c *Client) StrGet(ctx context.Context, key string) *StrCmd {
	args := make([]interface{}, 1)
	args[0] = key

	cmd := newStrCmd("strget")
	_ = cmdgroup.strCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) StrPut(ctx context.Context, key string, val string) *IntCmd {
	args := make([]interface{}, 2)
	args[0] = key
	args[1] = val

	cmd := newIntCmd("strput", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) StrDel(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 1)
	args[0] = key

	cmd := newIntCmd("strdel", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) IntGet(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 1)
	args[0] = key

	cmd := newIntCmd("intget", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) IntPut(ctx context.Context, key string, val int) *IntCmd {
	args := make([]interface{}, 2)
	args[0] = key
	args[1] = val

	cmd := newIntCmd("intput", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) IntDel(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 2)
	args[0] = key

	cmd := newIntCmd("intdel", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

// Pass an interface?
// Support []string
// map[string]string
// map[string]interface{} (for POD types)
// {key, value}, {key, value}, {key, value} pairs
func (c *Client) MapGet(ctx context.Context, key string, val map[string]string) *MapCmd {
	args := make([]interface{}, 2)
	args[0] = key
	args[1] = val

	cmd := newMapCmd("mapget", args...)
	_ = cmdgroup.mapCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) MapPut(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 1)
	args[0] = key

	cmd := newIntCmd("mapput", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (c *Client) MapDel(ctx context.Context, key string) *IntCmd {
	args := make([]interface{}, 1)
	args[0] = key

	cmd := newIntCmd("mapdel", args...)
	_ = cmdgroup.intCbtable[cmd.name](c, ctx, cmd)

	return cmd
}

func (b *baseCmd) Err() error {
	return b.err
}

func helloCommand(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	return nil
}

func echoCommand(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	val := cmd.args[0].(string)
	url := fmt.Sprintf("http://%s/%s", c.settings.Endpoint, cmd.name)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewBufferString(val))
	if err != nil {
		cmd.err = err
		return cmd
	}
	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(val)))
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("User-Agent", "kvs-client")
	resp, err := c.httpClient.Do(req)
	if err != nil && err != io.EOF {
		cmd.err = err
		return cmd
	} // TODO: Handle resp.Status != 200
	bytes, _ := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	cmd.result = string(bytes)
	return cmd
}

func stringGetCommand(c *Client, ctx context.Context, cmd *StrCmd) *StrCmd {
	return nil
}

func stringPutCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	return nil
}

func stringDelCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	return nil
}

func intGetCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	return nil
}

func intPutCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	return nil
}

func intDelCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	return nil
}

func mapGetCommand(c *Client, ctx context.Context, cmd *MapCmd) *MapCmd {
	key := cmd.args[0].(string)
	url := c.baseURL.JoinPath(key)
	result := makeRequest(c, ctx, "GET", url.String(), nil)
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
	key := cmd.args[0].(string)
	hashmap := cmd.args[1].(map[string]string)
	data, err := json.Marshal(hashmap)
	url := c.baseURL.JoinPath(key)
	result := makeRequest(c, ctx, "PUT", url.String(), bytes.NewBuffer(data))
	if result.err != nil {
		cmd.err = err
		return cmd
	}
	return cmd
}

func mapDelCommand(c *Client, ctx context.Context, cmd *IntCmd) *IntCmd {
	key := cmd.args[0].(string)
	url := c.baseURL.JoinPath(key)
	result := makeRequest(c, ctx, "DELETE", url.String(), nil)
	if result.err != nil {
		cmd.err = result.err
	}
	cmd.result = result.code
	return cmd
}

type makeReqResult struct {
	status   string
	code     int
	contents []byte
	err      error
}

// PUT | GET | DELETE
func makeRequest(c *Client, ctx context.Context, method string, URL string, contents *bytes.Buffer) *makeReqResult {
	result := &makeReqResult{}
	netURL, err := url.Parse(URL)
	if err != nil {
		result.err = err
		return result
	}
	req, err := http.NewRequestWithContext(ctx, method, netURL.String(), contents)
	if err != nil {
		result.err = err
		return result
	}
	if method == "PUT" {
		req.Header.Add("Content-Length", fmt.Sprintf("%d", contents.Len()))
		req.Header.Add("Content-Type", "text/plain") // should be application/json for a map
	}
	req.Header.Add("User-Agent", "kvs-client")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		result.err = err
		return result
	}
	if method == "GET" {
		bytes, err := io.ReadAll(resp.Body)
		defer resp.Body.Close()
		result.err = err
		result.contents = bytes
	}
	result.status = resp.Status
	result.code = resp.StatusCode
	return result
}
