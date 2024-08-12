package txn

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

type TransactionLogger interface {
	ReadTransactions() (<-chan *apitypes.Transaction, <-chan error)
	WriteTransaction(*apitypes.Transaction)
	HandleTransactions() <-chan error
}

type FileTransactionLogger struct {
	// TODO: Implement
}

type PostgresTransactionLogger struct {
	connPool     *pgxpool.Pool
	transactChan chan *apitypes.Transaction
}

type ServiceSettings struct {
}

type TransactionService struct {
	TransactionLogger
}

func NewTransactionService(logger TransactionLogger) *TransactionService {
	return &TransactionService{
		TransactionLogger: logger,
	}
}

func NewFileTransactionLogger(filePath string) (*FileTransactionLogger, error) {
	// TODO: Not implemented yet.
	return nil, nil
}

func NewPostgresTransactionLogger() (*PostgresTransactionLogger, error) {
	// TODO: Get databse URL from the environment
	dbConfig, err := pgxpool.ParseConfig("postgresql://postgres:nastish@postgres-db:5432/postgres?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to build a config %v", err)
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, err
	}

	if err := dbpool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to establish db connection %v", err)
	}

	logger := &PostgresTransactionLogger{
		connPool:     dbpool,
		transactChan: make(chan *apitypes.Transaction),
	}

	if err := logger.createTables(); err != nil {
		log.Logger.Error("Failed to create tables %v", err)
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) createTables() error {
	conn, err := l.connPool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("txn(postgres): failed to acquire database connection %v", err)
	}
	defer conn.Release()

	// Int table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "int_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE, 
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create int keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "integer_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL,
			"key_id" SERIAL,
			"value" INTEGER,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "int_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create integer transactions table %v", err)
		}
	}

	// Uint table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create uint keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_transactions" (
			"id" SERIAL, 
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"value" SERIAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "uint_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create uint transactions table %v", err)
		}
	}

	// Floats table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create float keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"value" REAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "float_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create float transactions table %v", err)
		}
	}

	// String table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create string keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL, 
			"value" TEXT, 
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "string_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create string transactions table %v", err)
		}
	}

	// Map table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_keys" (
			"id" SERIAL,
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create map keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "map_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create map transactions table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_key_value_pairs" (
			"transaction_id" SERIAL NOT NULL,
			"map_key_id" SERIAL NOT NULL,
			"key" TEXT NOT NULL,
			"value" TEXT NOT NULL,
			FOREIGN KEY("map_key_id") REFERENCES "map_keys"("id"),
			FOREIGN KEY("transaction_id") REFERENCES "map_transactions"("id"));`); err != nil {

			return fmt.Errorf("txn(postgres): failed to create map key-value pairs table %v", err)
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) readTransactions(dbConn *pgxpool.Conn, dbQuery string, transactChan chan<- *apitypes.Transaction, transactStorageType apitypes.TransactionStorageType) error {
	rows, _ := dbConn.Query(context.Background(), dbQuery)
	transactions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*apitypes.Transaction, error) {
		var (
			transact *apitypes.Transaction = &apitypes.Transaction{StorageType: transactStorageType}
			txnType  string
		)
		err := row.Scan(&transact.Timestamp, &txnType, &transact.Key, &transact.Data)
		if err == nil {
			transact.TxnType = apitypes.TransactionType(apitypes.TransactionTypeValue[txnType])
			if transact.Data != nil {
				// SERIAL in PostgreSQL is treated as int32, not unsigned integers,
				// so we would have to do a manual cast
				if transact.StorageType == apitypes.StorageUint {
					transact.Data = (uint32)(transact.Data.(int32))
				}
				// log type name
				log.Logger.Info("txn(postgres): read transaction %v, data type %s", transact.String(),
					reflect.TypeOf(transact.Data).Name(),
				)
			} else {
				log.Logger.Info("txn(postgres): read transaction %v", transact.String())
			}
		}
		return transact, err
	})

	if err != nil {
		log.Logger.Info("txn(postgres): failed to read transactions")
		return err
	}

	if len(transactions) > 0 {
		for _, transact := range transactions {
			transactChan <- transact
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) ReadTransactions() (<-chan *apitypes.Transaction, <-chan error) {
	transactChan := make(chan *apitypes.Transaction)
	errorChan := make(chan error) // only 1 error?

	go func() {
		defer close(transactChan)
		defer close(errorChan)

		log.Logger.Info("txn(postgres): start reading transactions")

		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("failed to acquire database connection from the poool %v", err)
			return
		}
		defer dbConn.Release()

		// Query Int transactions
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "integer_transactions" JOIN "int_keys" ON "int_keys"."id" = "integer_transactions"."key_id"
				WHERE "integer_transactions"."key_id" IN (
					SELECT "id" FROM "int_keys"
				);`

			err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageInt)
			if err != nil {
				log.Logger.Error("txn(postgres): failed to read int transactions %v", err)
				errorChan <- fmt.Errorf("txn(postgres): failed to read int transactions %v", err)
				return
			}
		}

		// Query Uint transactions
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "uint_transactions" JOIN "uint_keys" ON "uint_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "uint_keys"
				);`

			err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageUint)
			if err != nil {
				log.Logger.Error("txn(postgres): failed to read uint transactions %v", err)
				errorChan <- fmt.Errorf("tn(postgres): failed to read uint transactions %v", err)
				return
			}
		}

		// Query float transactions
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "float_transactions" JOIN "float_keys" ON "float_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "float_keys"
				);`

			err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageFloat)
			if err != nil {
				log.Logger.Error("txn(postgres): failed to read float transactions %v", err)
				errorChan <- fmt.Errorf("txn(postgres): failed to read float transactions %v", err)
				return
			}
		}

		// Query String transactions
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value" 
				FROM "string_transactions" JOIN "string_keys" ON "string_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "string_keys"
				);`

			err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageString)
			if err != nil {
				log.Logger.Error("txn(postgres): failed to read string transactions %v", err)
				errorChan <- fmt.Errorf("txn(postgres): failed to read string transactions %v", err)
				return
			}
		}

		// Query Map transactions
		{
			type MapTransactMD struct {
				TransactId   int32
				TransactType apitypes.TransactionType
				MapKeyId     int32
				MapKey       string
				Timestamp    time.Time
			}

			rows, _ := dbConn.Query(context.Background(),
				`SELECT "map_transactions"."id" AS "transact_id", "transaction_type", 
				"map_keys"."id" AS "map_key_id", "key", "timestamp" FROM "map_transactions"
				JOIN "map_keys" ON "key_id" = "map_keys"."id";`)

			mdArray, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*MapTransactMD, error) {
				var (
					md           = MapTransactMD{}
					transactType string
				)
				err := row.Scan(&md.TransactId, &transactType, &md.MapKeyId, &md.MapKey, &md.Timestamp)
				if err == nil {
					md.TransactType = apitypes.TransactionType(apitypes.TransactionTypeValue[transactType])
				}
				return &md, err
			})
			if err != nil {
				log.Logger.Error("txn(postgres): failed to read map transaction metadata %v", err)
				errorChan <- fmt.Errorf("txn(postgres): failed to read map transaction metadata %v", err)
				return
			}

			if len(mdArray) > 0 {
				transactions := make([]*apitypes.Transaction, 0)
				batch := &pgx.Batch{}
				for _, md := range mdArray {
					if md.TransactType == apitypes.TransactionPut { // or IncrBy or Incr
						qq := batch.Queue(
							`SELECT "key", "value" FROM "map_key_value_pairs"
							WHERE "transaction_id" = ($1) AND "map_key_id" = ($2);`, md.TransactId, md.MapKeyId)
						qq.Query(func(rows pgx.Rows) error {
							data := make(map[string]string)
							// NOTE: Once the storage supports maps of any kind, we could use pgx.RowToMap instead
							_, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]string, error) {
								var (
									key   string
									value string
								)
								err := row.Scan(&key, &value)
								if err == nil {
									data[key] = value
								}
								return nil, err
							})
							if err == nil {
								transactions = append(transactions, &apitypes.Transaction{
									StorageType: apitypes.StorageMap,
									TxnType:     md.TransactType,
									Timestamp:   md.Timestamp,
									Key:         md.MapKey,
									Data:        data,
								})
							}
							return err
						})
					} else {
						batch.Queue(`SELECT "id" FROM "map_key_value_pairs";`).QueryRow(func(row pgx.Row) error {
							transactions = append(transactions, &apitypes.Transaction{
								StorageType: apitypes.StorageMap,
								TxnType:     md.TransactType,
								Timestamp:   md.Timestamp,
								Key:         md.MapKey,
							})
							return nil
						})
					}
				}
				err := dbConn.SendBatch(context.Background(), batch)
				if err != nil {
					log.Logger.Info("txn(postgres): failed to read ")
					errorChan <- fmt.Errorf("txn(postgres): failed to read map transactions %v", err)
					return
				}

				for _, transact := range transactions {
					transactChan <- transact
				}
			}
		}

		// Make a signal that we're done with reading transactions.
		errorChan <- io.EOF

		log.Logger.Info("txn(postgres): finished reading transactions")
	}()

	return transactChan, errorChan
}

func (l *PostgresTransactionLogger) WriteTransaction(transact *apitypes.Transaction) {
	l.transactChan <- transact
}

func (l *PostgresTransactionLogger) insertTransactionKey(dbConn *pgxpool.Conn, transact *apitypes.Transaction, table string) (*int32, error) {
	// Get key's id from the table if exists, if it doesn't, insert it into a table and get its id
	query := fmt.Sprintf(`SELECT "id" FROM "%s" WHERE "key" = ($1)`, table)
	rows, _ := dbConn.Query(context.Background(), query, transact.Key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil {
		if err == pgx.ErrNoRows {
			// Insert new key into keys table and return an id
			query := fmt.Sprintf(`INSERT INTO "%s" ("key") values ($1) RETURNING "id";`, table)
			rows, err = dbConn.Query(context.Background(), query, transact.Key)
			keyId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return nil, fmt.Errorf("failed to insert key into a table %s, %v", table, err)
			}
		} else {
			return nil, fmt.Errorf("failed to retrive key id from a table %s, %v", table, err)
		}
	}

	return &keyId, nil
}

func (l *PostgresTransactionLogger) HandleTransactions() <-chan error {
	errorChan := make(chan error)

	go func() {
		log.Logger.Info("txn(postgres): start processing incoming transactions")
		defer log.Logger.Info("txn(postgres): finished processing incoming transactions")

		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("txn(postgres): failed to acquire database connection from pool %v", err)
			return
		}
		defer dbConn.Release()

		for {
			select {
			case transact := <-l.transactChan:
				switch transact.StorageType {
				case apitypes.StorageInt:
					keyId, err := l.insertTransactionKey(dbConn, transact, "int_keys")
					if err != nil {
						log.Logger.Error("txn(postgres): failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "integer_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data,
					)

					if err != nil {
						log.Logger.Error("txn(postgres): failed to write a int transaction %v", err)
						errorChan <- fmt.Errorf("txn(postgres): failed to write int transaction %v", err)
						return
					}

					log.Logger.Info("txn(postgres): wrote transaction %v", transact.String())

				case apitypes.StorageUint:
					keyId, err := l.insertTransactionKey(dbConn, transact, "uint_keys")
					if err != nil {
						log.Logger.Error("txn(postgres): failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "uint_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data,
					)

					if err != nil {
						log.Logger.Error("txn(postgres): failed to write a uint transaction %v", err)
						errorChan <- fmt.Errorf("txn(postgres): failed to write uint transaction %v", err)
						return
					}

					log.Logger.Info("txn(postgres): wrote transaction %v", transact.String())

				case apitypes.StorageFloat:
					keyId, err := l.insertTransactionKey(dbConn, transact, "float_keys")
					if err != nil {
						log.Logger.Error("txn(postgres): failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "float_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data,
					)

					if err != nil {
						log.Logger.Error("txn(postgres): failed to write a float transaction %v", err)
						errorChan <- fmt.Errorf("txn(postgres): failed to write float transaction %v", err)
						return
					}

					log.Logger.Info("txn(postgres): wrote transaction %v", transact.String())

				case apitypes.StorageString:
					keyId, err := l.insertTransactionKey(dbConn, transact, "string_keys")
					if err != nil {
						log.Logger.Error("txn(postgres): failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "string_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data,
					)

					if err != nil {
						log.Logger.Error("txn(postgres): failed to write a string transaction %v", err)
						errorChan <- fmt.Errorf("txn(postgres): failed to write string transaction %v", err)
						return
					}

					log.Logger.Info("txn(postgres): wrote transaction %v", transact.String())

				case apitypes.StorageMap:
					keyId, err := l.insertTransactionKey(dbConn, transact, "map_keys")
					if err != nil {
						log.Logger.Error("txn(postgres): failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					// NOTE: Maybe it's possible to use nested insert
					rows, _ := dbConn.Query(context.Background(),
						`INSERT INTO "map_transactions" ("timestamp", "transaction_type", "key_id") VALUES ($1, $2, $3) RETURNING "id";`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
					)
					transactId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
					if err != nil {
						log.Logger.Error("txn(postgres): failed to query transaction id %v", err)
						errorChan <- fmt.Errorf("txn(postgres): failed to query transaction id %v", err)
						return
					}

					if transact.Data != nil {
						batch := &pgx.Batch{}
						for key, value := range transact.Data.(map[string]string) {
							batch.Queue(
								`INSERT INTO "map_key_value_pairs" ("transaction_id", "map_key_id", "key", "value") 
								VALUES ($1, $2, $3, $4);`,
								transactId, *keyId, key, value)
						}

						err = dbConn.SendBatch(context.Background(), batch).Close()
						if err != nil {
							log.Logger.Error("txn(postgres): failed to insert map key value pairs %v", err)
							errorChan <- fmt.Errorf("txn(postgres): failed to insert into map key value pairs table %v", err)
							return
						}
					}

					log.Logger.Info("txn(postgres): wrote transaction %v", transact.String())
				}
			}
		}
	}()

	return errorChan
}

////////////////////////////////////////////////////////////////////////////////////////////
// This should be moved into its own package

func (l *FileTransactionLogger) ReadTransactions() (<-chan *apitypes.Transaction, <-chan error) {
	return nil, nil
}

func (l *FileTransactionLogger) WriteTransaction(*apitypes.Transaction) {

}

func (l *FileTransactionLogger) HandleTransactions() <-chan error {
	return nil
}
