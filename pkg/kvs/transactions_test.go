package kvs

import (
	_ "context"
	"os"
	"strconv"
	"testing"
	_ "time"

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

func populatePostgresWithTransactions(logger *PostgresTransactionLogger) {
	for i := 0; i < 10; i++ {
		key := "_entry_" + strconv.Itoa(i)
		logger.WriteTransaction(eventPut, storageInt, key, (i+1)<<2)
	}
}

// func TestInitPostgresTransactionLogger(t *testing.T) {
// 	defer goleak.VerifyNone(t)

// 	logger, err := newDBTransactionLogger(PostgresSettings{
// 		host:     "localhost",
// 		port:     5432,
// 		dbName:   "postgres",
// 		userName: "postgres",
// 		userPwd:  "12345",
// 	})
// 	assert.NoError(t, err)

// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer func() {
// 		cancel()
// 		logger.WaitForPendingTransactions()
// 	}()

// 	go logger.ProcessTransactions(ctx)

// 	time.Sleep(200 * time.Millisecond)

// 	populatePostgresWithTransactions(logger)

// 	// Wait a bit until the transaction will be added into a database
// 	time.Sleep(2 * time.Second)

// 	eventList := []Event{}

// 	events, errors := logger.ReadEvents()
// 	for {
// 		select {
// 		case event := <-events:
// 			eventList = append(eventList, event)
// 		case err := <-errors:
// 			t.Error(err)
// 		}
// 	}
// }
