package kvs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	_ "github.com/stretchr/testify/assert"

	"go.uber.org/goleak"

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

// This function will wait for pending transactions to complete
// We can pass a context to processTransactions()
// logger.WaitForPendingTransactions()
// {
// }

func TestInitPostgresTransactionLogger(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger, err := newDBTransactionLogger(PostgresSettings{
		host:     "localhost",
		port:     5432,
		dbName:   "postgres",
		userName: "postgres",
		userPwd:  "12345",
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	loggerDone := make(chan bool, 1)
	go func() {
		logger.processTransactions(ctx)
		close(loggerDone)
	}()
	time.Sleep(200 * time.Millisecond)

	logger.writeTransaction(eventAdd, storageTypeInt, "n", 12)
	logger.writeTransaction(eventGet, storageTypeInt, "n", nil)

	// Wait a bit until the transaction will be added into a database
	time.Sleep(400 * time.Millisecond)

	// TODO: Use the API of the transaction logger instead
	// logger.readEvents()

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer queryCancel()

	query := `SELECT event_type, key, value FROM integers_table;`
	rows, err := logger.db.QueryContext(queryCtx, query)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	var eventTypeStr string
	var hashKey string
	var value int

	for rows.Next() {
		if err := rows.Scan(&eventTypeStr, &hashKey, &value); err != nil {
			t.Error(err)
		}
		// assert.Equal(t, eventAdd, strToEvent[eventTypeStr])
		// assert.Equal(t, "n", hashKey)
		// assert.Equal(t, 12, value)
	}

	if rows.Err() != nil {
		t.Error(err)
	}

	cancel()
	<-loggerDone
}
