package txn_service

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

// NOTE: If we encountered Delete transaction, we have to remove all the transactions in a database prior to this one.
// Since the value no longer exists in the storage. That way we prevent transaction log growing exponentially.
// TODO: If key already exists, and the transaction type is PUT, use UPDATE instead of INSERT.

type TransactionLogger interface {
	ReadTransactions() (<-chan *apitypes.Transaction, <-chan error)
	WriteTransaction(*apitypes.Transaction)
	HandleTransactions() <-chan error
	Close()
}

type FileTransactionLogger struct {
	// TODO: Implement
}

type PostgresTransactionLogger struct {
	connPool     *pgxpool.Pool
	transactChan chan *apitypes.Transaction
	doneChan     chan bool
	ctx          context.Context
	cancelFunc   context.CancelFunc
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
	return nil, nil
}

func NewPostgresTransactionLogger(postgresUrl string) (*PostgresTransactionLogger, error) {
	dbConfig, err := pgxpool.ParseConfig(postgresUrl)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse config %v", err)
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, err
	}

	if err := dbpool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("Failed to establish connection %v", err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	logger := &PostgresTransactionLogger{
		connPool:     dbpool,
		transactChan: make(chan *apitypes.Transaction),
		doneChan:     make(chan bool),
		ctx:          ctx,
		cancelFunc:   cancelFunc,
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
		return fmt.Errorf("Failed to acquire database connection %v", err)
	}
	defer conn.Release()

	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "int_keys" (
			"id" SERIAL,
			"key" TEXT NOT NULL UNIQUE, 
			PRIMARY KEY("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create int keys table %v", err)
		}
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "int_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL,
			"key_id" SERIAL,
			"value" INTEGER,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "int_keys"("id") ON DELETE CASCADE);`,
		); err != nil {
			return fmt.Errorf("Failed to create integer transactions table %v", err)
		}
	}
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create uint keys table %v", err)
		}
		// NOTE: In postgresql SERIAL has a default non-null constraint.
		// INTEGER is too small for unsigned 32 bit int.
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL,
			"key_id" SERIAL,
			"value" BIGINT,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "uint_keys"("id") ON DELETE CASCADE);`,
		); err != nil {
			return fmt.Errorf("Failed to create uint transactions table %v", err)
		}
	}
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create float keys table %v", err)
		}
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"value" REAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "float_keys"("id") ON DELETE CASCADE);`,
		); err != nil {
			return fmt.Errorf("Failed to create float transactions table %v", err)
		}
	}
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create string keys table %v", err)
		}
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL,
			"key_id" SERIAL, 
			"value" TEXT, 
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "string_keys"("id") ON DELETE CASCADE);`,
		); err != nil {
			return fmt.Errorf("Failed to create string transactions table %v", err)
		}
	}
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_keys" (
			"id" SERIAL,
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create map keys table %v", err)
		}
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "map_keys"("id") ON DELETE CASCADE);`,
		); err != nil {
			return fmt.Errorf("Failed to create map transactions table %v", err)
		}
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_key_value_pairs" (
			"transaction_id" SERIAL NOT NULL,
			"map_key_id" SERIAL NOT NULL,
			"key" TEXT NOT NULL,
			"value" TEXT NOT NULL,
			FOREIGN KEY("map_key_id") REFERENCES "map_keys"("id"),
			FOREIGN KEY("transaction_id") REFERENCES "map_transactions"("id"));`,
		); err != nil {
			return fmt.Errorf("Failed to create map key-value pairs table %v", err)
		}
	}
	return nil
}

func (l *PostgresTransactionLogger) readTransactions(dbConn *pgxpool.Conn, dbQuery string, transactChan chan<- *apitypes.Transaction, transactStorageType apitypes.TransactionStorageType) error {
	rows, _ := dbConn.Query(context.Background(), dbQuery)
	transactions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*apitypes.Transaction, error) {
		var transact *apitypes.Transaction = &apitypes.Transaction{StorageType: transactStorageType}
		var txnType string

		err := row.Scan(&transact.Timestamp, &txnType, &transact.Key, &transact.Data)
		if err == nil {
			transact.TxnType = apitypes.TransactionType(apitypes.TransactionTypeValue[txnType])
			if transact.Data != nil {
				// NOTE: Postgre's INTEGER type is a signed integer, which has to be manually casted to uint32
				if transact.StorageType == apitypes.StorageUint {
					transact.Data = (uint32)(transact.Data.(int64))
				}
			}
		}
		return transact, err
	})
	if err != nil {
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
	errorChan := make(chan error)

	go func() {
		defer close(transactChan)
		defer close(errorChan)

		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("Failed to acquire database connection from the poool %v", err)
			return
		}
		defer dbConn.Release()

		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "int_transactions" JOIN "int_keys" ON "int_keys"."id" = "int_transactions"."key_id"
				WHERE "int_transactions"."key_id" IN (
					SELECT "id" FROM "int_keys"
				);`

			if err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageInt); err != nil {
				errorChan <- fmt.Errorf("Failed to read int transactions %v", err)
				return
			}
		}
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "uint_transactions" JOIN "uint_keys" ON "uint_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "uint_keys"
				);`

			if err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageUint); err != nil {
				errorChan <- fmt.Errorf("Failed to read uint transactions %v", err)
				return
			}
		}
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "float_transactions" JOIN "float_keys" ON "float_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "float_keys"
				);`

			if err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageFloat); err != nil {
				errorChan <- fmt.Errorf("Failed to read float transactions %v", err)
				return
			}
		}
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value" 
				FROM "string_transactions" JOIN "string_keys" ON "string_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "string_keys"
				);`

			if err := l.readTransactions(dbConn, query, transactChan, apitypes.StorageString); err != nil {
				errorChan <- fmt.Errorf("Failed to read string transactions %v", err)
				return
			}
		}
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

			mdArray, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (MapTransactMD, error) {
				var md = MapTransactMD{}
				var transactType string

				err := row.Scan(&md.TransactId, &transactType, &md.MapKeyId, &md.MapKey, &md.Timestamp)
				if err == nil {
					md.TransactType = apitypes.TransactionType(apitypes.TransactionTypeValue[transactType])
				}
				return md, err
			})
			if err != nil {
				errorChan <- fmt.Errorf("Failed to read map transaction metadata %v", err)
				return
			}

			if len(mdArray) > 0 {
				transactions := make([]*apitypes.Transaction, 0)
				batch := &pgx.Batch{}
				for _, md := range mdArray {
					if md.TransactType == apitypes.TransactionPut { // or IncrBy or Incr
						qq := batch.Queue(`SELECT "key", "value" FROM "map_key_value_pairs" WHERE "transaction_id" = ($1) AND "map_key_id" = ($2);`, md.TransactId, md.MapKeyId)
						qq.Query(func(rows pgx.Rows) error {
							data := make(map[string]string)
							_, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]string, error) {
								var key string
								var value string
								err := row.Scan(&key, &value)
								if err == nil {
									data[key] = value
								}
								return nil, err
							})
							if err != nil {
								return err
							}
							transactions = append(transactions, &apitypes.Transaction{StorageType: apitypes.StorageMap, TxnType: md.TransactType, Timestamp: md.Timestamp, Key: md.MapKey, Data: data})
							return nil
						})
					} else {
						// NOTE: This query won't return any key-value pairs since it's not a put/incrby transaction.
						// But, we need to make it anyway to read transactions in the right order, thus, get transactions,
						// cannot be read before put transactions etc. The reason for that is because all batch callbacks are executed
						// when we call Close() function on a batch result.
						batch.Queue(`SELECT * FROM "map_key_value_pairs" WHERE "transaction_id" = ($1) AND "map_key_id" = ($2);`, md.TransactId, md.MapKeyId).QueryRow(func(row pgx.Row) error {
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
				err := dbConn.SendBatch(context.Background(), batch).Close()
				if err != nil {
					errorChan <- fmt.Errorf("Failed to read map transactions %v", err)
					return
				}
				for _, transact := range transactions {
					transactChan <- transact
				}
			}
		}
		// Make a signal that we're done with reading transactions.
		errorChan <- io.EOF
	}()

	return transactChan, errorChan
}

func (l *PostgresTransactionLogger) WriteTransaction(transact *apitypes.Transaction) {
	l.transactChan <- transact
}

func (l *PostgresTransactionLogger) insertTransactionKey(dbConn *pgxpool.Conn, transact *apitypes.Transaction, table string) (*int32, error) {
	query := fmt.Sprintf(`SELECT "id" FROM "%s" WHERE "key" = ($1)`, table)
	rows, _ := dbConn.Query(context.Background(), query, transact.Key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil {
		if err == pgx.ErrNoRows {
			query := fmt.Sprintf(`INSERT INTO "%s" ("key") values ($1) RETURNING "id";`, table)
			rows, err = dbConn.Query(context.Background(), query, transact.Key)
			keyId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return nil, fmt.Errorf("Failed to insert key into a table %s, %v", table, err)
			}
		} else {
			return nil, fmt.Errorf("Failed to retrive key id from a table %s, %v", table, err)
		}
	}

	return &keyId, nil
}

func (l *PostgresTransactionLogger) deleteTransactions(dbConn *pgxpool.Conn, keysTableName, transactionsTableName, key string) error {
	// If DELETE transaction is received, we should delete all prior transactions from a database.
	// TODO: Look up (ON DELETE CASCADE), that should do the trick instead of making two separate queries
	rows, _ := dbConn.Query(context.Background(), fmt.Sprintf(`DELETE FROM "%s" WHERE "key" = ($1) RETURNING "id";`, keysTableName), key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil {
		return fmt.Errorf("Failed to delete key %s from table %s, error %v", key, keysTableName, err)
	}

	_, err = dbConn.Exec(context.Background(), fmt.Sprintf(`DELETE FROM "%s" WHERE "key_id" = ($1);`, transactionsTableName), keyId)
	if err != nil {
		return fmt.Errorf("Failed to delete transactions from table %s, error %v", transactionsTableName, err)
	}

	return nil
}

func (l *PostgresTransactionLogger) HandleTransactions() <-chan error {
	errorChan := make(chan error)

	go func() {
		defer close(l.doneChan)
		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("Failed to acquire database connection %v", err)
			return
		}
		defer dbConn.Release()

		for {
			select {
			case <-l.ctx.Done():
				if len(l.transactChan) > 0 {
					// TODO: Finish writing pending transactions
				}
				return

			case transact := <-l.transactChan:
				var keyId *int32
				var err error

				switch transact.StorageType {
				case apitypes.StorageInt:
					if keyId, err = l.insertTransactionKey(dbConn, transact, "int_keys"); err != nil {
						errorChan <- err
						return
					}
					if transact.TxnType == apitypes.TransactionDel {
						if err = l.deleteTransactions(dbConn, "int_keys", "int_transactions", transact.Key); err != nil {
							errorChan <- err
							return
						}
					} else {
						if _, err := dbConn.Exec(context.Background(),
							`INSERT INTO "int_transactions" ("timestamp", "transaction_type", "key_id", "value")
							VALUES ($1, $2, $3, $4);`,
							transact.Timestamp, apitypes.TransactionTypeName[int32(transact.TxnType)], *keyId, transact.Data,
						); err != nil {
							errorChan <- fmt.Errorf("txn(postgres): failed to write int transaction %v", err)
							return
						}
					}

				case apitypes.StorageUint:
					if keyId, err = l.insertTransactionKey(dbConn, transact, "uint_keys"); err != nil {
						errorChan <- err
						return
					}
					if _, err := dbConn.Exec(context.Background(),
						`INSERT INTO "uint_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp, apitypes.TransactionTypeName[int32(transact.TxnType)], *keyId, transact.Data,
					); err != nil {
						errorChan <- fmt.Errorf("Failed to write uint transaction %v", err)
						return
					}

				case apitypes.StorageFloat:
					if keyId, err = l.insertTransactionKey(dbConn, transact, "float_keys"); err != nil {
						errorChan <- err
						return
					}
					if _, err := dbConn.Exec(context.Background(),
						`INSERT INTO "float_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp, apitypes.TransactionTypeName[int32(transact.TxnType)], *keyId, transact.Data,
					); err != nil {
						errorChan <- fmt.Errorf("Failed to write float transaction %v", err)
						return
					}

				case apitypes.StorageString:
					if keyId, err = l.insertTransactionKey(dbConn, transact, "string_keys"); err != nil {
						errorChan <- err
						return
					}
					if _, err = dbConn.Exec(context.Background(),
						`INSERT INTO "string_transactions" ("timestamp", "transaction_type", "key_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp, apitypes.TransactionTypeName[int32(transact.TxnType)], *keyId, transact.Data,
					); err != nil {
						errorChan <- fmt.Errorf("Failed to write string transaction %v", err)
						return
					}

				case apitypes.StorageMap:
					if keyId, err = l.insertTransactionKey(dbConn, transact, "map_keys"); err != nil {
						errorChan <- err
						return
					}
					rows, _ := dbConn.Query(context.Background(),
						`INSERT INTO "map_transactions" ("timestamp", "transaction_type", "key_id") VALUES ($1, $2, $3) RETURNING "id";`,
						transact.Timestamp, apitypes.TransactionTypeName[int32(transact.TxnType)], *keyId,
					)
					transactId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
					if err != nil {
						errorChan <- fmt.Errorf("Failed to query map transaction id %v", err)
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
							errorChan <- fmt.Errorf("Failed to insert into map key value pairs table %v", err)
							return
						}
					}
				}
			}
		}
	}()

	return errorChan
}

func (l *PostgresTransactionLogger) Close() {
	l.cancelFunc()
	<-l.doneChan
}

// //////////////////////////////////////////////////////////////////////////////////////////
// This should be moved into its own package
func (l *FileTransactionLogger) ReadTransactions() (<-chan *apitypes.Transaction, <-chan error) {
	return nil, nil
}

func (l *FileTransactionLogger) WriteTransaction(*apitypes.Transaction) {

}

func (l *FileTransactionLogger) HandleTransactions() <-chan error {
	return nil
}

func (l *FileTransactionLogger) Close() {
}
