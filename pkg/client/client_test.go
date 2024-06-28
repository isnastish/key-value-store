package kvs

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/mux"
	_ "go.uber.org/goleak"

	"github.com/stretchr/testify/assert"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/version"
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

	const val int = 9999997
	const key = "id"

	putRes := client.IntAdd(ctx, key, val)
	assert.True(t, putRes.Error() == nil)
	getRes := client.IntGet(ctx, key)
	assert.True(t, getRes.Error() == nil)
	assert.Equal(t, val, getRes.Result())
	delRes := client.IntDel(ctx, key)
	assert.True(t, delRes.Error() == nil)
	*getRes = IntCmd{}
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
	*getRes = FloatCmd{}
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
	*getRes = StrCmd{}
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
	*getRes = MapCmd{}
	getRes = client.MapGet(ctx, key)
	assert.True(t, getRes.Error() != nil)
}

func Test_IntIncr(t *testing.T) {

}

func Test_IntIncBy(t *testing.T) {

}

func Test_CancelRequestIfServerIsHanding(t *testing.T) {
	// Use the development server here
	// defer goleak.VerifyNone(t)

	// Creating a subroute migth not be necessary
	router := mux.NewRouter()
	subRouter := router.PathPrefix(fmt.Sprintf("/api/%s/", version.GetServiceVersion())).Subrouter()
	subRouter.Path("/echo").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		empty := make(chan bool, 1)
		<-empty
	}).Methods("PUT")

	go func() {
		// TODO: Figure out how to tear down http server gracefully
		log.Logger.Info("Listening %s", settings.Endpoint)
		if err := http.ListenAndServe(settings.Endpoint, router); err != nil {
			log.Logger.Panic("Server fatal error %v", err)
			assert.True(t, false)
		}
	}()

	client := NewClient(settings)
	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()

	echoRes := client.Echo(ctx, "eChO EcHo ECHO echo")
	log.Logger.Info("Echo result %v", echoRes.Result())
}

func Test_TrailingSlashPointsToTheSameRoute(t *testing.T) {

}
