package kvs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	_ "go.uber.org/goleak"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/testing"
)

var endpoint = "127.0.0.1:8080"

func reset(v interface{}) {
	switch v.(type) {
	case *IntCmd:
		*(v.(*IntCmd)) = IntCmd{}
	case *BoolCmd:
		*(v.(*BoolCmd)) = BoolCmd{}
	case *StrCmd:
		*(v.(*StrCmd)) = StrCmd{}
	case *FloatCmd:
		*(v.(*FloatCmd)) = FloatCmd{}
	case *MapCmd:
		*(v.(*MapCmd)) = MapCmd{}
	default:
		panic("Invalid type")
	}
}

func echo(src string) string {
	res := []rune(src)
	for i := 0; i < len(res); i++ {
		if unicode.IsLetter(res[i]) {
			if unicode.IsLower(res[i]) {
				res[i] = unicode.ToUpper(res[i])
				continue
			}
			res[i] = unicode.ToLower(res[i])
		}
	}
	return string(res)
}

func TestMain(m *testing.M) {
	var status int
	var kill bool

	defer func() {
		if kill {
			testutil.KillKVSServiceContainer()
		}
		os.Exit(status)
	}()

	kill, err := testutil.StartKVSServiceContainer()
	if err != nil {
		log.Logger.Error("Failed to start docker container %v", err)
		return
	}
	status = m.Run()
}

func TestEcho(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	res := client.Echo(context.Background(), "EcHo")
	assert.True(t, res.Error() == nil)
	assert.Equal(t, "eChO", res.Result())
}

func TestHello(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	res := client.Hello(context.Background())
	assert.True(t, res.Error() == nil)
	log.Logger.Info("Result %s", res.Result())
	assert.True(t, strings.Contains(res.Result(), "kvs"))
}

func TestFibo(t *testing.T) {
	// defer goleak.VerifyNone(t)
	// Fibo rpc is a great way of testing request cancelation with a context
	// So, in the future I should use context.WithTimeout here
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	// indices:       0  1  2  3  4  5  6  7   8   9
	// fibo sequence: 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
	fiboRes := client.Fibo(context.Background(), 7)
	assert.True(t, fiboRes.Error() == nil)
	assert.Equal(t, 13, fiboRes.Result())

	reset(fiboRes)

	{
		// Hit context deadline before computing the value
		n := 500
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
		defer cancel()
		fiboRes = client.Fibo(timeoutCtx, n)
		assert.True(t, fiboRes.Error() == context.DeadlineExceeded)
	}
}

func TestIntRoundtrip(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithTimeout(context.Background(), 20000*time.Millisecond)
	defer cancel()

	const val int = 9999997
	const key = "id"

	putRes := client.IntAdd(ctx, key, val)
	assert.True(t, putRes.Error() == nil)
	getRes := client.IntGet(ctx, key)
	assert.True(t, getRes.Error() == nil)
	assert.Equal(t, val, getRes.Result())
	delRes := client.IntDel(ctx, key)
	assert.True(t, delRes.Error() == nil)
	reset(getRes)
	getRes = client.IntGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func TestFloatRoundtrip(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const val float32 = 2.71828
	const key = "epsilon"

	putRes := client.F32Add(ctx, key, val)
	assert.True(t, putRes.Error() == nil)
	getRes := client.F32Get(ctx, key)
	assert.True(t, getRes.Error() == nil)
	assert.Equal(t, val, getRes.Result())
	delRes := client.F32Del(ctx, key)
	assert.True(t, delRes.Error() == nil)
	assert.True(t, delRes.Result())
	reset(getRes)
	getRes = client.F32Get(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func TestStringRoundtrip(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const val string = "Hello! This is a test string. I could have computed a checksum using MD5 or SHA256 algorithms here, but I am too lazzzzzy"
	const key = "dummy_String"

	putRes := client.StrAdd(ctx, key, val)
	assert.True(t, putRes.Error() == nil)
	getRes := client.StrGet(ctx, key)
	assert.True(t, getRes.Error() == nil)
	assert.Equal(t, val, getRes.Result())
	delRes := client.StrDel(ctx, key)
	assert.True(t, delRes.Error() == nil)
	assert.True(t, delRes.Result())
	reset(getRes)
	getRes = client.StrGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func TestHashMapRoundtrip(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	val := map[string]string{"first_entry": "fff0xx", "second_entry": "RRRRRR"}
	const key = "randomMap"

	putRes := client.MapAdd(ctx, key, val)
	assert.True(t, putRes.Error() == nil)
	getRes := client.MapGet(ctx, key)
	assert.True(t, getRes.Error() == nil)
	assert.Equal(t, val, getRes.Result())
	delRes := client.MapDel(ctx, key)
	assert.True(t, delRes.Error() == nil)
	assert.True(t, delRes.Result())
	reset(getRes)
	getRes = client.MapGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func TestIntIncr(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const key = "messsageId"
	incrRes := client.IntIncr(ctx, key)
	assert.Equal(t, 0, incrRes.Result())
	reset(incrRes)
	incrRes = client.IntIncr(ctx, key)
	assert.Equal(t, 1, incrRes.Result())
	reset(incrRes)

	// 1024 http request... And open field for optimizations (put them into a single http request)
	for i := 2; i < 1026; i++ {
		incrRes = client.IntIncr(ctx, key)
		assert.Equal(t, i, incrRes.Result())
		reset(incrRes)
	}
	delRes := client.Del(ctx, key)
	assert.True(t, delRes.Result())
}

func TestIntIncBy(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: endpoint, RetriesCount: 3}
	client := NewClient(&settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const key = "number"
	incrRes := client.IntIncrBy(ctx, key, 64)
	assert.Equal(t, 0, incrRes.Result())
	reset(incrRes)
	incrRes = client.IntIncrBy(ctx, key, 3)
	assert.Equal(t, 64, incrRes.Result())
	delRes := client.Del(ctx, key)
	assert.True(t, delRes.Result())
}

func TestRetries(t *testing.T) {
	// NOTE: This test doesn't utilize the common docker container running the KVS service
	// It uses a different concept, mock server, which is executed in a separate goroutine.
	// defer goleak.VerifyNone(t)
	handlerHitCount := 0
	settings := Settings{Endpoint: "127.0.0.1:6060", RetriesCount: 3}
	server := testutil.NewMockServer(settings.Endpoint)
	server.BindHandler("/echo", http.MethodPost, func(w http.ResponseWriter, req *http.Request) {
		log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", req.RequestURI, req.Method, req.RemoteAddr)
		if handlerHitCount == (settings.RetriesCount - 1) {
			bytes, _ := io.ReadAll(req.Body)
			defer req.Body.Close()
			res := echo(string(bytes))
			log.Logger.Info(res)
			w.Header().Add("Content-Length", fmt.Sprintf("%d", len(res)))
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res))
			return
		}
		w.WriteHeader(http.StatusTooEarly)
		handlerHitCount++
	})

	server.Start()
	defer server.Kill()

	client := NewClient(&settings)
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancel()

	src := "ECHO ECHo ECho Echo echo"
	expected := echo(src)
	echoRes := client.Echo(ctx, src)
	assert.True(t, echoRes.Error() == nil)
	assert.Equal(t, expected, echoRes.Result())
}

func TestContextDeadlineOnRetries(t *testing.T) {
	// defer goleak.VerifyNone(t)
	settings := Settings{Endpoint: "127.0.0.1:6060", RetriesCount: 5}
	server := testutil.NewMockServer(settings.Endpoint)
	server.BindHandler("/echo", http.MethodPost, func(w http.ResponseWriter, req *http.Request) {
		// http://..../path?status=http.StatusTooEarly
		log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", req.RequestURI, req.Method, req.RemoteAddr)
		w.WriteHeader(http.StatusTooEarly)
	})

	server.Start()
	defer server.Kill()

	client := NewClient(&settings)
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()

	echoRes := client.Echo(ctx, "ECHO")
	assert.Equal(t, context.DeadlineExceeded, echoRes.Error())
}
