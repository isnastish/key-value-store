package kvs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isnastish/kvs/pkg/log"
)

// NOTE: If the value has been deleted, hence "delete" event was sent.
// We have to clean up all the transactions, otherwise our transaction storage will
// keep growing and we eventually run out of memory or ids to enumerate the transactions.

type PostgresSettings struct {
	host     string
	dbName   string
	userName string
	userPwd  string
	port     int
}

type PostgresTransactionLogger struct {
	eventsChan chan<- Event
	errorsChan <-chan error
	dbpool     *pgxpool.Pool
	wg         sync.WaitGroup
}

func newDBTransactionLogger(settings PostgresSettings) (*PostgresTransactionLogger, error) {
	const DATABASE_URL = "postgresql://postgres:nastish@127.0.0.1:5432/postgres?sslmode=disable"

	dbconfig, err := pgxpool.ParseConfig(DATABASE_URL)
	if err != nil {
		return nil, fmt.Errorf("failed to build a config %w", err)
	}

	dbconfig.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		log.Logger.Info("Before acquiring connection from the pool")
		return true
	}

	dbconfig.AfterRelease = func(conn *pgx.Conn) bool {
		log.Logger.Info("After releasing connection from the pool")
		return true
	}

	dbconfig.BeforeClose = func(conn *pgx.Conn) {
		log.Logger.Info("Closing the connection")
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		return nil, err
	}

	if err := dbpool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to establish db connection %w", err)
	}

	logger := &PostgresTransactionLogger{dbpool: dbpool, wg: sync.WaitGroup{}}

	if err := logger.createTablesIfDontExist(); err != nil {
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) WaitForPendingTransactions() {
	l.wg.Wait()
}

func (l *PostgresTransactionLogger) createTablesIfDontExist() error {
	timeout := 15 * time.Second

	dbconn, err := l.dbpool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire db connection %w", err)
	}
	defer dbconn.Release()

	// TODO: Figure out how to create a type if it doesn't exist
	// {
	// 	// Create a separate enum event type for storing events
	// 	ctx, cancel = context.WithTimeout(context.Background(), ctxTimeout)
	// 	defer cancel()
	// 	query = `create type if not exists "event_type" as enum ('get', 'delete', 'put', 'update');`
	// 	if _, err = l.db.ExecContext(ctx, query); err != nil {
	// 		return fmt.Errorf("failed to create event type %w", err)
	// 	}
	// }

	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		query := `create table if not exists "integer_storage" (
			"id" serial primary key,
			"event" event_type not null,
			"key" text not null,
			"value" integer,
			"inserttime" timestamp not null default now());`
		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to crete integers table %w", err)
		}
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		query := `create table if not exists "float_storage" (
			"id" serial primary key,
			"event" event_type not null,
			"key" text not null,
			"value" numeric,
			"inserttime" timestamp not null default now());`
		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to crete floats table %w", err)
		}
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		query := `create table if not exists "string_storage" (
			"id" serial primary key,
			"event" event_type not null,
			"key" text not null,
			"value" text, 
			"inserttime" timestamp not null default now());`
		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create strings table %w", err)
		}
	}

	// {
	// 	// NOTE: A separate table should be created for map's key-values.
	// 	// Otherwise we end up having a lot of redundancies.
	// 	ctx, cancel = context.WithTimeout(context.Background(), ctxTimeout)
	// 	defer cancel()
	// 	query = `create table if not exists "hashmap_storage" (
	// 		"id" serial primary key,
	// 		"event" event_type not null,
	// 		"hash" text not null unique,
	// 		"inserttime" timestamp not null default now());`
	// 	if _, err = l.db.ExecContext(ctx, query); err != nil {
	// 		return fmt.Errorf("failed to create maps table %w", err)
	// 	}
	// }

	// {
	// 	// NOTE: Use foreign key instead of using hashes from the parent table.
	// 	ctx, cancel = context.WithTimeout(context.Background(), ctxTimeout)
	// 	defer cancel()
	// 	query = `create table if not exists "hashmap_data" (
	// 		"id" serial primary key,
	// 		"hash" text not null unique,
	// 		"key" text not null,
	// 		"value" text,
	// 		primary key("id"));`
	// }

	return nil
}

func (l *PostgresTransactionLogger) WriteTransaction(eventType EventType, storage StorageType, key string, value interface{}) {
	// NOTE: Timestamp is used for file transactions only.
	// For Postgre we maintain a inserttime column which is default to now().
	// So, whenever the transactions is inserted, a timestamp computed automatically.
	l.eventsChan <- Event{storageType: storage, t: eventType, key: key, value: value}
}

func insertTransactionIntoDB(dbconn *pgxpool.Conn, event *Event) error {
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch event.storageType {
	case storageInt:
		if event.t == eventPut {
			query := `insert into "integer_storage" ("event", "key", "value") values ($1, $2, $3);`
			_, err = dbconn.Exec(ctx, query, event.t, event.key, event.value.(int64))
		} else {
			query := `insert into "integer_storage" ("event", "key") values ($1, $2);`
			_, err = dbconn.Exec(ctx, query, event.t, event.key)
		}
		if err != nil {
			return fmt.Errorf("failed to insert transaction into integer storage %w", err)
		}

	case storageFloat:
		if event.t == eventPut {
			query := `insert into "float_storage" ("event", "key", "value") values ($1, $2, $3);`
			_, err = dbconn.Exec(ctx, query, event.t, event.value.(float32))
		} else {
			query := `insert into "float_storage" ("event", "key") values ($1, $2);`
			_, err = dbconn.Exec(ctx, query, event.t, event.key)
		}
		if err != nil {
			return fmt.Errorf("failed to insert transaction into float storage %w", err)
		}

	case storageString:
		if event.t == eventPut {
			query := `insert into "string_storage" ("event", "key", "value") values ($1, $2, $3);`
			_, err = dbconn.Exec(ctx, query, event.t, event.key, event.value.(string))
		} else {
			query := `insert into "string_storage" ("event", "key") values ($1, $2);`
			_, err = dbconn.Exec(ctx, query, event.t, event.key)
		}
		if err != nil {
			return fmt.Errorf("failed to insert transaction into string storage %w", err)
		}

		// case storageTypeMap:
		// 	query = `INSERT INTO "map_table" VALUES ($1, $2, $3, $4, $5);`
		// 	key := event.Key
		// 	if event.Type == eventAdd {
		// 		hashTable := event.Val.(map[string]string)
		// 		for k, v := range hashTable {
		// 			if _, err := l.db.ExecContext(ctx, query, event.Id,
		// 				eventToStr[event.Type], key, k, v,
		// 			); err != nil {
		// 				return err
		// 			}
		// 		}
		// 	} else {
		// 		_, err = l.db.ExecContext(ctx, query, event.Id,
		// 			eventToStr[event.Type], key, "", "",
		// 		)
		// 	}
	}
	return err
}

func (l *PostgresTransactionLogger) ProcessTransactions(ctx context.Context) {
	eventsChan := make(chan Event, 32)
	errorsChan := make(chan error, 1)

	l.eventsChan = eventsChan
	l.errorsChan = errorsChan

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		// This is the best place to close a connections pool.
		// Once all the write completed.
		defer l.dbpool.Close()

		dbconn, err := l.dbpool.Acquire(context.Background())
		if err != nil {
			errorsChan <- fmt.Errorf("failed to acquire db connection %w", err)
			return
		}

		// Release the acquired connection first, before closing the database connection.
		defer dbconn.Release()

		for {
			select {
			case event := <-eventsChan:
				if err := insertTransactionIntoDB(dbconn, &event); err != nil {
					errorsChan <- err
					return
				}

			case <-ctx.Done():
				if len(eventsChan) > 0 {
					for event := range eventsChan {
						if err := insertTransactionIntoDB(dbconn, &event); err != nil {
							errorsChan <- err
							return
						}
					}
				}
				return
			}
		}
	}()
}

func selectEventsFromTable(dbconn *pgxpool.Conn, storage StorageType, table string, eventsChan chan<- Event, errorsChan chan<- error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	query := fmt.Sprintf(`select * from "%s";`, table)
	rows, _ := dbconn.Query(ctx, query)
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
		event := Event{storageType: storage}
		err := row.Scan(&event.id, &event.t, &event.key, &event.value, &event.timestamp)
		return event, err
	})
	if err != nil {
		err = fmt.Errorf("failed to select events for %s table %w", table, err)
		errorsChan <- err
		return err
	}

	for _, event := range events {
		log.Logger.Info("Read event: Event{id: %d, t: %s, key: %s, value: %v, timestamp: %s}",
			event.id, event.t, event.key, event.value, event.timestamp.Format(time.DateTime))

		eventsChan <- event
	}

	return nil
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	eventsChan := make(chan Event)
	errorsChan := make(chan error, 1)

	go func() {
		defer close(eventsChan)
		defer close(errorsChan)

		dbconn, err := l.dbpool.Acquire(context.Background())
		if err != nil {
			errorsChan <- fmt.Errorf("failed to acquire db connection %w", err)
			return
		}
		defer dbconn.Release()

		if err := selectEventsFromTable(dbconn, storageInt, "integer_storage", eventsChan, errorsChan); err != nil {
			return
		}

		if err := selectEventsFromTable(dbconn, storageFloat, "float_storage", eventsChan, errorsChan); err != nil {
			return
		}

		if err := selectEventsFromTable(dbconn, storageString, "string_storage", eventsChan, errorsChan); err != nil {
			return
		}

		// {
		// 	query := `select * from "map_storage";`
		// 	ctx, cancel = context.WithTimeout(context.Background(), ctxTimeout)
		// 	defer cancel()

		// 	rows, err := l.db.QueryContext(ctx, query)
		// 	if err != nil {
		// 		errors <- fmt.Errorf("failed to select data from string storage %w", err)
		// 		return
		// 	}
		// 	for rows.Next() {
		// 		var value string
		// 		if err := rows.Scan(&event, &key, &value); err != nil {
		// 			errors <- err
		// 			return
		// 		}
		// 		events <- Event{StorageType: storageTypeInt, Type: strToEvent[event], Key: key, Val: value}
		// 	}
		// 	if rows.Err() != nil {
		// 		errors <- rows.Err()
		// 	}
		// }
		// TODO: Add map implementation
	}()

	return eventsChan, errorsChan
}
