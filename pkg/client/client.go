package kvs

import (
	"net/http"
)

type Settings struct {
	Addr string
}

type client struct {
	settings   *Settings
	httpClient http.Client
}

func NewClient(settings *Settings) *client {
	return &client{}
}

// It should return some error code
func (c *client) Put(hashkey string, key string, value string) {

}
