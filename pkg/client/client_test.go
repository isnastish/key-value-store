package kvs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "go.uber.org/goleak"
)

var settings = &Settings{
	Endpoint:     ":8080",
	RetriesCount: 3,
}

func zero(v interface{}) {
	switch v.(type) {
	case *IntCmd:
		(*v.(*IntCmd)) = IntCmd{}
	case *BoolCmd:
		(*v.(*BoolCmd)) = BoolCmd{}
	case *StrCmd:
		(*v.(*StrCmd)) = StrCmd{}
	case *FloatCmd:
		(*v.(*FloatCmd)) = FloatCmd{}
	case *MapCmd:
		(*v.(*MapCmd)) = MapCmd{}
	default:
		panic("Invalid type")
	}
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

// func Test_KillServer(t *testing.T) {
// 	client := NewClient(settings)
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	killRes := client.Kill(ctx)
// 	assert.Equal(t, 200, killRes.StatusCode())
// }
