package kvs

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/testsetup"
)

const hostPort = 6060
const psqlPassword = "12345"

func writeTxnFromEvent(txnLogger *PostgresTxnLogger, event *Event) {
	txnLogger.WriteTransaction(event.txnType, event.storageType, event.key, event.value)
}

// This verficiation mechanism doesn't take into a consideration the timestamp
// If exactly the same event was inserted later, the code will break.
func hasEvent(eventList []Event, src *Event) bool {
	for _, event := range eventList {
		if event.key == src.key && event.txnType == src.txnType && event.storageType == src.storageType {
			if reflect.ValueOf(event.value).Equal(reflect.ValueOf(src.value)) {
				return true
			}
		}
	}
	return false
}

func TestMain(m *testing.M) {
	var tearDown bool
	var exitCode int

	defer func() {
		if tearDown {
			testsetup.KillPostgresContainer()
		}
		os.Exit(exitCode)
	}()

	tearDown, err := testsetup.StartPostgresContainer(hostPort, psqlPassword)
	if err != nil {
		log.Logger.Fatal("Failed to start Postgres container %v", err)
	}

	exitCode = m.Run()
}

func TestIntTransaction(t *testing.T) {
	databaseURL := fmt.Sprintf("postgresql://postgres:%s@localhost:%d/postgres?sslmode=disable", psqlPassword, hostPort)
	txnLogger, err := NewDBTxnLogger(databaseURL)
	if err != nil {
		assert.Nil(t, err)
		return
	}
	defer txnLogger.Close()

	ctx, cancel := context.WithCancel(context.Background())

	go txnLogger.ProcessTransactions(ctx)
	defer func() {
		cancel()
		txnLogger.WaitForPendingTransactions()
	}()

	i32Put := Event{storageType: storageInt, txnType: txnPut, key: "_i32_key", value: int32(1777998)}
	i32Get := Event{storageType: storageInt, txnType: txnGet, key: "_i32_key"}
	i32Del := Event{storageType: storageInt, txnType: txnDelete, key: "_i32_key"}

	writeTxnFromEvent(txnLogger, &i32Put)
	writeTxnFromEvent(txnLogger, &i32Get)
	writeTxnFromEvent(txnLogger, &i32Del)

	// f32PutEvent := Event{txnType: txnPut, key: "_f32_key", value: 3.14}
	// f32GetEvent := Event{txnType: txnGet, key: "_f32_key"}
	// f32DelEvent := Event{txnType: txnDelete, key: "_f32_key"}

	// Make sure that events we properly inserted into the database.
	// This is an emulation of a real-world scenario, where the data is already in a database
	// before starting reading it (ReadEvents() procedure).
	// So, before making any reads, we have to make sure that we have data available.
	<-time.After(2 * time.Second)

	eventList := []Event{}
	events, errors := txnLogger.ReadEvents()
	for running := true; running != false; {
		select {
		case event := <-events:
			eventList = append(eventList, event)

		case err := <-errors:
			if err == nil { // error channel was closed gracefully
				running = false
			} else {
				log.Logger.Fatal("Received error %v", err)
			}
		}
	}

	assert.Equal(t, 3, len(eventList))
	assert.True(t, hasEvent(eventList, &i32Put))
	assert.True(t, hasEvent(eventList, &i32Get))
	assert.True(t, hasEvent(eventList, &i32Del))
}

// Decided to separate testing map transactions from other types
// since they are the most complex.
func TestMapTransactions(t *testing.T) {

}
