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
	// NOTE: Use chan Event/chan error instead
	events chan Event
	errors chan error
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

	logger := &PostgresTransactionLogger{
		db: db,
		// events: make(chan Event),
		// errors: make(chan error),
	}

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
		// NOTE: storing event_type as a string for convenience (as opposite to integers),
		// so it's easy to inspect the data in a database
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
		);`

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
		// NOTE: maps_table should consist of two tables which are related
		// to each other by the hash
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
	// NOTE: Id is not set to avoid declaring it as atomic
	l.events <- Event{StorageType: storageType, Type: evenType, Key: key, Val: values, Timestamp: time.Now()}
}

func (l *PostgresTransactionLogger) insertEventIntoDB(event *Event) error {
	var query string

	queryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	switch event.StorageType {
	case storageTypeInt:
		query = `INSERT INTO integers_table VALUES (
			$1, $2, $3, $4, $5
		);`
		// TODO: Try out without a type cast
		if _, err := l.db.ExecContext(queryCtx, query,
			event.Id, eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(int),
		); err != nil {
			return err
		}

	case storageTypeFloat:
		query := `INSERT INTO floats_table VALUES (
			$1, $2, $3, $4, $5
		);`
		if _, err := l.db.ExecContext(
			queryCtx, query, event.Id, eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(float32),
		); err != nil {
			return err
		}

	case storageTypeString:
		query := `INSERT INTO strings_table (
			$1, $2, $3, $4, $5
		);`
		if _, err := l.db.ExecContext(
			queryCtx, query, event.Id, eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(string),
		); err != nil {
			return err
		}

	case storageTypeMap:
		// Insert everything into a single table for now. We can revise it on subsequent iterations
		m := event.Val.(map[string]string)
		hash := event.Key
		for k, v := range m {
			query := `INSERT INTO map_table VALUES (
				$1, $2, $3, $4, $5, $6
			);`
			if _, err := l.db.ExecContext(
				queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, hash, k, v,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) processTransactions(ctx context.Context) {
	// TODO: Close db connection at the end.
	events := make(chan Event, 32)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors

	var query string
	var err error

	for {
		select {
		case event := <-events:

			if err != nil {
				errors <- fmt.Errorf("failed to insert an event %w", err)
				return
			}

		case <-ctx.Done():
			// Finish writing pending events
			for event := range events {
				// insert
				_ = event
			}
			errors <- ctx.Err()
			return
		}
	}
}

func (l *PostgresTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	events := make(chan Event)
	errors := make(chan error, 1) // buffered error channel

	// NOTE: The reason why we have to read from a db in a separate goroutine,
	// is because the service will be receiving events and errors, to prevent blocking.

	go func() {
		// NOTE: Even though we close the channels before returning from the outer function,
		// the receiver will still be able to read the events from the closed channel.
		defer close(events)
		defer close(errors)

		// We can read simultaneously from multiple tables in a database by creating four separate go routines
		{
			// NOTE: This operation might take some time, so specifying the context of 10s
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			query := `SELECT event_type, key, value FROM integers_table;`
			rows, err := l.db.QueryContext(ctx, query)
			if err != nil {
				errors <- fmt.Errorf("failed to query data from integers_table, %w", err)
			}

			var eventTypeStr string
			var hashKey string
			var value int

			for rows.Next() {
				if err := rows.Scan(&eventTypeStr, &hashKey, &value); err != nil {
					errors <- err
					return
				}
				events <- Event{
					StorageType: storageTypeInt,
					Type:        strToEvent[eventTypeStr],
					Key:         hashKey, Val: value,
				}
			}

			if rows.Err() != nil {
				errors <- err
				return
			}
		}
	}()

	return events, errors
}
