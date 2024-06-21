package kvs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Endpoint_Echo(t *testing.T) {
	client := NewClient(&Settings{
		Endpoint:     ":8080",
		RetriesCount: 3,
	})
	res := client.Echo(context.Background(), "EcHo")
	assert.True(t, res.Error() == nil)
	assert.Equal(t, "eChO", res.Result())
}

func Test_Endpoint_Hello(t *testing.T) {
	client := NewClient(&Settings{
		Endpoint: ":8080",
	})
	res := client.Hello(context.Background())
	assert.True(t, res.Error() == nil)
	assert.Equal(t, "Hello from KVS service", res.Result())
}

// TODO: Explore random string generation
func Test_Endpoint_IntRoundtrip(t *testing.T) {
	client := NewClient(&Settings{
		Endpoint: ":8080",
	})
	const number = 9999997
	res := client.IntPut(context.Background(), "index", number)
	assert.True(t, res.Error() == nil)
	assert.Equal(t, number, res.Result())
}
