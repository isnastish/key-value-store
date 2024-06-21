// Run the kvc service inside the docker container in a seprate process
package kvs

import (
	"context"
	"testing"

	_ "github.com/stretchr/testify/assert"
)

func TestKVSEcho(t *testing.T) {
	client := NewClient(&Settings{Endpoint: ":8080", RetriesCount: 4})
	res := client.Echo(context.Background(), "hello")
	_ = res
	// assert.Equal(t, nil, res.Err())
	// assert.Equal(t, "HELLO", res.Val())
}
