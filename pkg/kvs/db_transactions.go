package kvs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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

	ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		// NOTE: Almost always use %w instead of %v and %s for error formating
		// https: //stackoverflow.com/questions/61283248/format-errors-in-go-s-v-or-w
		return nil, fmt.Errorf("failed to establish db connection %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	if err := logger.createTablesIfDontExist(); err != nil {
		db.Close()
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) dropTable(table string) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, table)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := l.db.ExecContext(ctx, query, table); err != nil {
		return err
	}
	return nil
}

func (l *PostgresTransactionLogger) createTablesIfDontExist() error {
	{
		ctx, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
		defer cancel()

		// NOTE: If the value is a map we should store it in a separate table,
		// We can create a table for ints floats and maps
		// id | event_type(PUT/GET/DELETE) | storage_type(Int/Float/String/Map)
		query := `CREATE TABLE IF NOT EXISTS kvs_transactions_table (
			id INTEGER PRIMARY KEY,
			event_type INTEGER,
			storage_type INTEGER
		);`
		_, err := l.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create transactions table %w", err)
		}
	}
	{
		query := `CREATE TABLE IF NOT EXISTS integers_table (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value INTEGER
		);`

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := l.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to crete integers table %w", err)
		}
	}
	{
		query := `CREATE TABLE IF NOT EXISTS floats_table (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value FLOAT
		);` // Maybe use double precision?

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := l.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to crete floats table %w", err)
		}
	}
	{
		query := `CREATE TABLE IF NOT EXISTS strings_table (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value TEXT
		);`
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := l.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create strings table %w", err)
		}
	}
	{
		query := `CREATE TABLE IF NOT EXISTS maps_table (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			hash_key TEXT UNIQUE,
			key TEXT UNIQUE,
			value TEXT
		);`
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := l.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create maps table %w", err)
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) writeTransaction(evenType EventType, storageType StorageType, key string, values ...interface{}) {

}

func (l *PostgresTransactionLogger) processTransactions(ctx context.Context) {

}

func (l *PostgresTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	return nil, nil
}
