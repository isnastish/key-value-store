package kvs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/isnastish/kvs/pkg/log"
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

	logger := &PostgresTransactionLogger{
		db: db,
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
	var (
		err         error
		query       string
		queryCtx    context.Context
		queryCancel context.CancelFunc

		ctxTimeout = 10 * time.Second
	)

	{
		queryCtx, queryCancel = context.WithTimeout(context.Background(), ctxTimeout)
		defer queryCancel()
		query = `CREATE TABLE IF NOT EXISTS kvs_transactions_table (
			id INTEGER,
			event_type INTEGER,
			storage_type INTEGER, 
			PRIMARY KEY("id"));`
		if _, err = l.db.ExecContext(queryCtx, query); err != nil {
			return fmt.Errorf("failed to create transactions table %w", err)
		}
	}
	{
		queryCtx, queryCancel = context.WithTimeout(context.Background(), ctxTimeout)
		defer queryCancel()
		query = `CREATE TABLE IF NOT EXISTS integers_table (
			id INTEGER,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value INTEGER,
			PRIMARY KEY("id"));`
		if _, err = l.db.ExecContext(queryCtx, query); err != nil {
			return fmt.Errorf("failed to crete integers table %w", err)
		}
	}
	{
		queryCtx, queryCancel = context.WithTimeout(context.Background(), ctxTimeout)
		defer queryCancel()
		query = `CREATE TABLE IF NOT EXISTS floats_table (
			id INTEGER,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value FLOAT,
			PRIMARY KEY("id"));`
		if _, err = l.db.ExecContext(queryCtx, query); err != nil {
			return fmt.Errorf("failed to crete floats table %w", err)
		}
	}
	{
		queryCtx, queryCancel = context.WithTimeout(context.Background(), ctxTimeout)
		defer queryCancel()
		query = `CREATE TABLE IF NOT EXISTS strings_table (
			id INTEGER,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			key TEXT UNIQUE,
			value TEXT, 
			PRIMARY KEY("id"));`
		if _, err = l.db.ExecContext(queryCtx, query); err != nil {
			return fmt.Errorf("failed to create strings table %w", err)
		}
	}
	{
		// NOTE: Idealy, maps_table should consist of two tables which are related
		// to each other by the hash
		queryCtx, queryCancel = context.WithTimeout(context.Background(), ctxTimeout)
		defer queryCancel()
		query = `CREATE TABLE IF NOT EXISTS maps_table (
			id INTEGER,
			event_type VARCHAR(64),
			time_stamp TIMESTAMP,
			hash_key TEXT UNIQUE,
			key TEXT UNIQUE,
			value TEXT,
			PRIMARY KEY("id"));`
		if _, err = l.db.ExecContext(queryCtx, query); err != nil {
			return fmt.Errorf("failed to create maps table %w", err)
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) writeTransaction(evenType EventType, storageType StorageType, key string, value interface{}) {
	// NOTE: At this point, events channel should be initialized,
	// which is done in processTransactions procedure
	l.events <- Event{StorageType: storageType, Type: evenType, Key: key, Val: value, Timestamp: time.Now()}
}

func (l *PostgresTransactionLogger) insertEventIntoDB(event *Event) error {
	var query string
	var err error

	queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch event.StorageType {
	case storageTypeInt:
		query = "INSERT INTO integers_table VALUES ($1, $2, $3, $4, $5);"
		if event.Type == eventAdd {
			_, err = l.db.ExecContext(queryCtx, query,
				event.Id, eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(int),
			)
		} else {
			_, err = l.db.ExecContext(queryCtx, query,
				event.Id, eventToStr[event.Type], event.Timestamp, event.Key, 0,
			)
		}

	case storageTypeFloat:
		query = "INSERT INTO floats_table VALUES ($1, $2, $3, $4, $5);"
		if event.Type == eventAdd {
			_, err = l.db.ExecContext(queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(float32),
			)
		} else {
			_, err = l.db.ExecContext(queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, event.Key, float32(0),
			)
		}

	case storageTypeString:
		query = "INSERT INTO strings_table ($1, $2, $3, $4, $5);"
		if event.Val == eventAdd {
			_, err = l.db.ExecContext(queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, event.Key, event.Val.(string))
		} else {
			_, err = l.db.ExecContext(queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, event.Key, "")
		}

	case storageTypeMap:
		query = "INSERT INTO map_table VALUES ($1, $2, $3, $4, $5, $6);"
		key := event.Key
		if event.Type == eventAdd {
			hashTable := event.Val.(map[string]string)
			for k, v := range hashTable {
				if _, err := l.db.ExecContext(queryCtx, query, event.Id,
					eventToStr[event.Type], event.Timestamp, key, k, v,
				); err != nil {
					return err
				}
			}
		} else {
			_, err = l.db.ExecContext(queryCtx, query, event.Id,
				eventToStr[event.Type], event.Timestamp, key, "", "",
			)
		}
	}
	return err
}

func (l *PostgresTransactionLogger) processTransactions(ctx context.Context) {
	defer l.db.Close()

	events := make(chan Event, 32)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors

	for {
		select {
		case event := <-events:
			log.Logger.Info("Got event, inserting into the database")
			if err := l.insertEventIntoDB(&event); err != nil {
				errors <- err
				return
			}

		case <-ctx.Done():
			if len(events) > 0 {
				log.Logger.Info("Writing pending events")
				for event := range events {
					if err := l.insertEventIntoDB(&event); err != nil {
						errors <- err
						return
					}
				}
			}
			return
		}
	}
}

func (l *PostgresTransactionLogger) readEvents() (<-chan Event, <-chan error) {
	events := make(chan Event)
	errors := make(chan error, 1)

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
					Type:        strToEvent[eventTypeStr], // Should always match once the database was properly added.
					Key:         hashKey,
					Val:         value,
				}
			}

			if rows.Err() != nil {
				errors <- err
				return
			}
		}
		{
			// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)

			// var eventType string
			// var mapHashKey string
			// var m map[string]string

			// query := `SELECT (event_type, hash_key, key, value) FROM maps_table;`
			// rows, err := l.db.QueryContext(ctx, query)

			// for rows.Next() {

			// }
		}
	}()

	return events, errors
}
