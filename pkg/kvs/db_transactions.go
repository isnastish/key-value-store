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

	// Once we establish clear table structure (schema), we can remove them.
	intKeyTable      string
	intTxnTable      string
	floatKeyTable    string
	floatTxnTable    string
	strKeyTable      string
	strTxnTable      string
	mapHashTable     string
	mapTxnTable      string
	mapKeyValueTable string
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

	logger := &PostgresTransactionLogger{
		dbpool: dbpool,
		wg:     sync.WaitGroup{},
		// keep a list of table names in case we need to change them,
		// we can do it in one place, and it's easy to drop all the table at once
		intKeyTable:      "int_keys",
		intTxnTable:      "int_transactions",
		floatKeyTable:    "float_keys",
		floatTxnTable:    "float_transactions",
		strKeyTable:      "str_keys",
		strTxnTable:      "str_transactions",
		mapHashTable:     "map_hashes",
		mapTxnTable:      "map_transactions",
		mapKeyValueTable: "map_key_values",
	}

	if err := logger.createTablesIfDontExist(); err != nil {
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) WaitForPendingTransactions() {
	l.wg.Wait()
}

func (l *PostgresTransactionLogger) dropTable(dbconn *pgxpool.Conn, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := dbconn.Exec(ctx, fmt.Sprintf(`drop table if exists "%s";`, table)); err != nil {
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
		if err := createTxnKeyTable(dbconn, l.intKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" integer,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.intTxnTable, l.intKeyTable)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", l.intTxnTable, err)
		}
	}
	// float transactions
	{
		if err := createTxnKeyTable(dbconn, l.floatKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" numeric,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.floatTxnTable, l.floatKeyTable)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", l.floatKeyTable, err)
		}
	}
	// string transactions
	{
		if err := createTxnKeyTable(dbconn, l.strKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" text,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.strTxnTable, l.strKeyTable)

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to crete %s table %w", l.strTxnTable, err)
		}
	}
	// map transactions
	{

		if err := createTxnKeyTable(dbconn, l.mapHashTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"map_id" integer,
			"key" text not null,
			"value" text not null,
			foreign key("map_id") references "%s"("id") on delete cascade);`, l.mapKeyValueTable, l.mapHashTable)

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", l.mapKeyValueTable, err)
		}
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"map_id" integer, 
			"timestampt" timestamp not null default now(),
			foreign key("map_id") references "%s"("id") on delete cascade);`, l.mapTxnTable, l.mapHashTable)

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create map transactions table %w", err)
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

func extractKeyId(dbconn *pgxpool.Conn, table string, event *Event) (int32, error) {
	rows, _ := dbconn.Query(context.Background(), fmt.Sprintf(`select "id" from "%s" where "key" = ($1);`, table), event.key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil { // either `key` doesn't exist, or an error occured
		if err == pgx.ErrNoRows { // `key` doesn't exist
			rows, err = dbconn.Query(context.Background(), fmt.Sprintf(`insert into "%s" ("key") values ($1) returning "id";`, table), event.key)
			keyId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return 0, fmt.Errorf("failed to insert key into %s, %w", table, err)
			}
		} else {
			return 0, fmt.Errorf("failed to select key from %s, %w", table, err)
		}
	}
	return keyId, nil
}

func (l *PostgresTransactionLogger) insertTransactionIntoDB(dbconn *pgxpool.Conn, event *Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch event.storageType {
	case storageInt:
		keyId, err := extractKeyId(dbconn, l.intKeyTable, event)
		if err != nil {
			return err
		}

		var value interface{} = nil
		if event.t == eventPut {
			value = event.value.(int32)
		}
		if _, err = dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.intTxnTable), event.t, keyId, value); err != nil {
			return fmt.Errorf("failed to insert into %s table %w", l.intTxnTable, err)
		}

	case storageFloat:
		keyId, err := extractKeyId(dbconn, l.floatKeyTable, event)
		if err != nil {
			return err
		}

		var value interface{} = nil
		if event.t == eventPut {
			value = event.value.(float32)
		}
		if _, err = dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.floatTxnTable), event.t, keyId, value); err != nil {
			return fmt.Errorf("failed to insert into %s table %w", l.floatTxnTable, err)
		}

	case storageString:
		keyId, err := extractKeyId(dbconn, l.strKeyTable, event)
		if err != nil {
			return err
		}

		var value interface{} = nil
		if event.t == eventPut {
			value = event.value.(string)
		}
		if _, err := dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.strTxnTable), event.t, keyId, value); err != nil {
			return fmt.Errorf("failed to insert into %s table %w", l.strTxnTable, err)
		}

	case storageMap:
		hashId, err := extractKeyId(dbconn, l.mapHashTable, event)
		if err != nil {
			return err
		}
		_, err = dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "map_id") values ($1, $2);`, l.mapTxnTable), event.t, hashId)
		if err != nil {
			return fmt.Errorf("failed to insert into %s table %w", l.mapTxnTable, err)
		}

		// If the event contains data, insert that data into key-value table.
		// Queue all the queries and send them in a single batch.
		if event.t == eventPut {
			batch := &pgx.Batch{}
			for key, value := range event.value.(map[string]string) {
				batch.Queue(fmt.Sprintf(`inset into "%s" ("map_id", "key", "value") values ($1, $2, $3);`, l.mapKeyValueTable), hashId, key, value)
			}
			err = dbconn.SendBatch(context.Background(), batch).Close()
			if err != nil {
				return fmt.Errorf("failed to insert into %s table %w", l.mapKeyValueTable, err)
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
				if err := l.insertTransactionIntoDB(dbconn, &event); err != nil {
					errorsChan <- err
					return
				}

			case <-ctx.Done():
				if len(eventsChan) > 0 {
					for event := range eventsChan {
						if err := l.insertTransactionIntoDB(dbconn, &event); err != nil {
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
		err = fmt.Errorf("failed to select events from %s table %w", table, err)
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

		// if err := selectEventsFromTable(dbconn, storageInt, l.intTxnTable, eventsChan, errorsChan); err != nil {
		// 	return
		// }

		// if err := selectEventsFromTable(dbconn, storageFloat, l.floatTxnTable, eventsChan, errorsChan); err != nil {
		// 	return
		// }

		// if err := selectEventsFromTable(dbconn, storageString, l.strTxnTable, eventsChan, errorsChan); err != nil {
		// 	return
		// }

		// {
		// 	type HashBundle struct {
		// 		Id   int32
		// 		Hash string
		// 	}

		// 	rows, _ := dbconn.Query(context.Background(), fmt.Sprintf(`select ("id", "key") from "%s";`, l.mapHashTable))
		// 	hashes, err := pgx.CollectRows(rows, pgx.RowToStructByPos[HashBundle])
		// 	if err != nil {
		// 		errorsChan <- fmt.Errorf("failed to select map hashes %w", err)
		// 		return
		// 	}

		// 	_ = hashes
		// 	// batch := &pgx.Batch{}
		// 	// for b := range hashes {
		// 	// 	batch.Queue(`select ("event") from "map_storage" where "hash_id" = ($1);`, id)
		// 	// }
		// }
	}()

	return eventsChan, errorsChan
}
