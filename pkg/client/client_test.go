// Run the kvc service inside the docker container in a seprate process
package kvs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVSEcho(t *testing.T) {
	client := Client(&Settings{KvsEndpoint: ":8080"})
	res := client.Echo(context.Background(), "hello")
	assert.Equal(t, nil, res.Err())
	assert.Equal(t, "HELLO", res.Val())
}

func TestKVSSet(t *testing.T) {
	client := Client(&Settings{KvsEndpoint: ":8080"})
	_ = client
}
