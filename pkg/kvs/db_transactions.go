package kvs

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresSettings struct {
	host     string
	dbName   string
	userName string
	userPwd  string
	port     int
}

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

func newDBTransactionLogger(settings PostgresSettings) (*PostgresTransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", settings.host, settings.port, settings.dbName, settings.userName, settings.userPwd)

	// NOTE: Might not create a connection, to establish the connection Ping should be used
	// "postgres://username:password@localhost/db_name?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		// https: //stackoverflow.com/questions/61283248/format-errors-in-go-s-v-or-w
		return nil, fmt.Errorf("failed to establish db connection %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	return logger, nil
}

func (l *PostgresTransactionLogger) writeTransaction(evenType EventType, storageType StorageType, key string, values ...interface{}) {

}

func (l *PostgresTransactionLogger) processTransactions(ctx context.Context) {

}

func (l *PostgresTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
