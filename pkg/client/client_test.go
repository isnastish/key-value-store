// TODO: Explore random string generation
package kvs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

var settings = &Settings{
	Endpoint:     ":8080",
	RetriesCount: 3,
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

	const V = 9999997
	const key = "id"
	res := client.IntPut(ctx, key, V)
	assert.True(t, res.Error() == nil)
	res = client.IntGet(ctx, key)
	assert.True(t, res.Error() == nil)
	assert.Equal(t, V, res.Result())
	delRes := client.IntDel(ctx, key)
	assert.True(t, delRes.Error() == nil)
	// verify that the value was deleted
	res = client.IntGet(ctx, key)
	assert.True(t, res.Error() != nil)
}
