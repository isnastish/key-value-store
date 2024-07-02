package kvs

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/testing"
)

var settings = &Settings{
	Endpoint:   "127.0.0.1:8080",
	RetryCount: 3,
}

func zero(v interface{}) {
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

func Test_Echo(t *testing.T) {
	client := NewClient(settings)
	res := client.Echo(context.Background(), "EcHo")
	assert.True(t, res.Error() == nil)
	assert.Equal(t, "eChO", res.Result())
}

func Test_Hello(t *testing.T) {
	client := NewClient(settings)
	res := client.Hello(context.Background())
	assert.True(t, res.Error() == nil)
	assert.Equal(t, "Hello from KVS service", res.Result())
}

func Test_IntRoundtrip(t *testing.T) {
	client := NewClient(settings)
	ctx, cancel := context.WithCancel(context.Background())
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
	zero(getRes)
	getRes = client.IntGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func Test_FloatRoundtrip(t *testing.T) {
	client := NewClient(settings)
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
	zero(getRes)
	getRes = client.F32Get(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func Test_StringRoundtrip(t *testing.T) {
	client := NewClient(settings)
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
	zero(getRes)
	getRes = client.StrGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func Test_HashMapRoundtrip(t *testing.T) {
	client := NewClient(settings)
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
	zero(getRes)
	getRes = client.MapGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func Test_IntIncr(t *testing.T) {
	client := NewClient(settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zero := func(res *IntCmd) {
		*res = IntCmd{}
	}

	const key = "messsageId"
	incrRes := client.IntIncr(ctx, key)
	assert.Equal(t, 0, incrRes.Result())
	zero(incrRes)
	incrRes = client.IntIncr(ctx, key)
	assert.Equal(t, 1, incrRes.Result())
	zero(incrRes)

	// 1024 http request... And open field for optimizations (put them into a single http request)
	for i := 2; i < 1026; i++ {
		incrRes = client.IntIncr(ctx, key)
		assert.Equal(t, i, incrRes.Result())
		zero(incrRes)
	}
	delRes := client.Del(ctx, key)
	assert.True(t, delRes.Result())
}

func Test_IntIncBy(t *testing.T) {
	client := NewClient(settings)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const key = "number"
	incrRes := client.IntIncrBy(ctx, key, 64)
	assert.Equal(t, 0, incrRes.Result())
	zero(incrRes)
	incrRes = client.IntIncrBy(ctx, key, 3)
	assert.Equal(t, 64, incrRes.Result())
	delRes := client.Del(ctx, key)
	assert.True(t, delRes.Result())
}

func Test_Retries(t *testing.T) {
	defer goleak.VerifyNone(t)

	handlerHitCount := 0

	server := testutil.NewMockServer(settings.Endpoint, settings.RetryCount)
	server.BindHandler("/echo", http.MethodGet, func(w http.ResponseWriter, req *http.Request) {
		log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", req.RequestURI, req.Method, req.RemoteAddr)
		if handlerHitCount == (settings.RetryCount - 1) {
			bytes, _ := io.ReadAll(req.Body)
			defer req.Body.Close()
			res := echo(string(bytes))
			w.Write([]byte(res))
			return
		}
		w.WriteHeader(http.StatusTooEarly)
		handlerHitCount++
	})

	server.Start()
	defer server.Kill()

	client := NewClient(settings)
	ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Millisecond)
	defer cancel()

	src := "ECHO ECHo ECho Echo echo"
	expected := echo(src)
	echoRes := client.Echo(ctx, src)
	assert.True(t, echoRes.Error() == nil)
	assert.Equal(t, expected, echoRes.Result())
}
