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

func (l *PostgresTransactionLogger) dropTableIfExists(dbconn *pgxpool.Conn, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	query := fmt.Sprintf(`drop table if exists "%s";`, table)
	if _, err := dbconn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to drop the table %s, %w", table, err)
	}

	return nil
}

func (l *PostgresTransactionLogger) createTablesIfDontExist() error {
	timeout := 15 * time.Second

	dbconn, err := l.dbpool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire db connection %w", err)
	}
	defer dbconn.Release()

	createTxnKeyTable := func(dbconn *pgxpool.Conn, tableName string) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"key" text not null unique);`, tableName)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", tableName, err)
		}

		return nil
	}

	// TODO: Once we establish the structure of hwo the tables should look like,
	// we can factor them out into functions to avoid code duplications.
	// Maybe even create all the tables in a single batch?

	// integer transactions
	{
		if err := createTxnKeyTable(dbconn, "integer_keys"); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := `create table if not exists "integer_txns" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" integer,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "integer_keys"("id") on delete cascade);`

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create integer transaction table %w", err)
		}
	}
	// float transactions
	{
		if err := createTxnKeyTable(dbconn, "float_keys"); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := `create table if not exists "float_txns" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" numeric,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "float_keys"("id") on delete cascade);`

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create float transaction table %w", err)
		}
	}
	// string transactions
	{
		if err := createTxnKeyTable(dbconn, "string_keys"); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := `create table if not exists "string_txns" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" text,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "string_keys"("id") on delete cascade);`

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to crete string transactions table %w", err)
		}
	}
	// map transactions
	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// |------------------|
		// | id |     hash    |
		// |----|-------------|
		// | 0  |  "foo_hash" |
		// | 1  |  "bar_hash" |
		query := `create table if not exists "map_hash_ids" (
			"id" serial primary key, 
			"hash" text not null unique);`

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to created map subtable %w", err)
		}
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// |---------------------|
		// | id | event | map_id | <- id of the hash in a different table
		// |----|-------|--------|
		// | 0  | 'put' |   12   |
		// | 1  | 'put' |   14   |
		query := `create table if not exists "map_storage" (
			"id" serial primary key,
			"event" varchar(32) not null,
			"map_id" integer, 
			"timestampt" timestamp not null default now(), 
			foreign key("map_id") references "map_hash_ids"("id") on delete cascade);`

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create map storage %w", err)
		}
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// |---------------------------------------|
		// | id | map_id  |   key     |    value   |
		// |----|---------|-----------|------------|
		// |  0 |  12     | "foo_bar" |  "baz"     |
		query := `create table if not exists "map_key_value_pairs" (
			"map_id" integer,
			"key" text not null,
			"value" text not null, 
			foreign key("map_id") references "map_hash_ids"("id") on delete cascade);`

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create map key-value table %w", err)
		}
	}

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
			// `integer_keys` table should contain unique keys only,
			// thus we have to check whether the key exists first,
			// and insert it only if it doesn't.
			dbconn.Query(ctx, `insert into "integer_keys", `)
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

	case storageMap:
		// Get id of the hash if it already exists
		var hashId int32

		// NOTE: Create a separate context for each query?
		rows, _ := dbconn.Query(context.Background(), `select "hash_id" from "map_hash_ids" where "hash" = $1;`, event.key)
		hashIds, err := pgx.CollectRows(rows, pgx.RowTo[int32])
		if err != nil {
			return fmt.Errorf("failed to select hash ids %w", err)
		}
		if len(hashIds) == 0 {
			rows, _ = dbconn.Query(context.Background(), `insert into "map_hash_ids" ("hash") values ($1) returning "hash_id";`, event.key)
			hashId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return fmt.Errorf("failed to insert a new hash %s, %w", event.key, err)
			}
		} else {
			hashId = hashIds[0]
		}

		if _, err = dbconn.Exec(context.Background(), `insert into "map_storage" ("event", "hash_id") values ($1, $2);`, event.t, hashId); err != nil {
			return fmt.Errorf("failed to insert transaction into map storage %w", err)
		}

		if event.t == eventPut {
			// send all the queires in a single batch
			// Batches are transactional, meaning that they are implicitly wrapped into Begin/Commit
			batch := &pgx.Batch{}
			for key, value := range event.value.(map[string]string) {
				batch.Queue(`inset into "map_key_value_pairs" ("hash_id", "key", "value") values ($1, $2, $3);`, hashId, key, value)
			}
			err = dbconn.SendBatch(context.Background(), batch).Close()
			if err != nil {
				return fmt.Errorf("failed to insert into map key-value table %w", err)
			}
		}
	}

	return nil
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

	// TODO: If the query return n column, but the struct has n-1 columns,
	// use RowToStructByNameLax? But in that case, the Event sturct should have tags assigned to it.
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

		{
			type HashBundle struct {
				Id   int32
				Hash string
			}

			// Select all the hashes and their ids
			rows, _ := dbconn.Query(context.Background(), `select ("hash_id", "hash") from "map_hash_ids";`)
			hashes, err := pgx.CollectRows(rows, pgx.RowToStructByPos[HashBundle])
			if err != nil {
				errorsChan <- fmt.Errorf("failed to select map hashes %w", err)
				return
			}

			_ = hashes
			// batch := &pgx.Batch{}
			// for b := range hashes {
			// 	batch.Queue(`select ("event") from "map_storage" where "hash_id" = ($1);`, id)
			// }
		}
	}()

	return eventsChan, errorsChan
}
