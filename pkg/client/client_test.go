package kvs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEchoKVS(t *testing.T) {
	var settings Settings
	kvsclient := Client(&settings)
	res := kvsclient.Echo(context.Background(), "hello")
	assert.Equal(t, nil, res.Error())
	assert.Equal(t, "HELLO", res.Value())
}
