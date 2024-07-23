package kvs

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/testsetup"
)

const hostPort = 4040
const psqlPassword = "12345"

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

	const eventKey = "number"
	const eventValue int32 = 1777998

	txnLogger.WriteTransaction(txnPut, storageInt, "number", eventValue)
	txnLogger.WriteTransaction(txnGet, storageInt, "number", nil)

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
	assert.Equal(t, 2, len(eventList))
	assert.Equal(t, eventKey, eventList[0].key)
	assert.Equal(t, txnPut, eventList[0].txnType)
	assert.Equal(t, eventValue, eventList[0].value.(int32))
}
