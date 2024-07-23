package kvs

import (
	"context"
	"os"
	"testing"
	"time"

	_ "github.com/stretchr/testify/assert"

	_ "go.uber.org/goleak"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/testsetup"
)

func TestMain(m *testing.M) {
	var tearDown bool
	var exitCode int

	defer func() {
		if tearDown {
			testsetup.KillPostgresContainer()
		}
		os.Exit(exitCode)
	}()

	tearDown, err := testsetup.StartPostgresContainer()
	if err != nil {
		log.Logger.Fatal("Failed to start Postgres container %v", err)
	}

	exitCode = m.Run()
}

func TestIntTransaction(t *testing.T) {
	// defer goleak.VerifyNone(t)
	const DATABASE_URl = "postgresql://postgres:12345@localhost:4040/postgres?sslmode=disable"
	txnLogger, err := NewDBTxnLogger(DATABASE_URl)
	if err != nil {
		t.Error(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go txnLogger.ProcessTransactions(ctx)

	<-time.After(200 * time.Millisecond)

	txnLogger.WriteTransaction(txnPut, storageInt, "number", 1777998)
	txnLogger.WriteTransaction(txnGet, storageInt, "number", nil)

	eventList := []Event{}
	events, errors := txnLogger.ReadEvents()
	for {
		select {
		case event := <-events:
			eventList = append(eventList, event)
		case err := <-errors:
			t.Error(err)
		}
	}
}
