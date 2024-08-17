package txn_service

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/testsetup"
)

const postgresPort = 5050
const postgresPwd = "12345"
const postgresUrl = "postgresql://postgres:12345@localhost:5050/postgres?sslmode=disable"

func TestMain(m *testing.M) {
	var tearDown bool
	var exitCode int

	defer func() {
		if tearDown {
			testsetup.KillPostgresContainer()
		}
		os.Exit(exitCode)
	}()

	tearDown, err := testsetup.StartPostgresContainer(5050, "12345")
	if err != nil {
		fmt.Printf("Failed to start Postgres container %v\n", err)
		os.Exit(1)
	}

	exitCode = m.Run()
}

func transactionsEqual(a, b *apitypes.Transaction) (bool, error) {
	if a.StorageType != b.StorageType {
		return false, fmt.Errorf("Storage type mismatch: Expected: %v\n Received: %v\n", a.StorageType, b.StorageType)
	}
	if a.TxnType != b.TxnType {
		return false, fmt.Errorf("Transaction type mismatch: Expected: %v\n Received: %v\n", a.TxnType, b.TxnType)
	}
	if a.Key != b.Key {
		return false, fmt.Errorf("Key mismatch: Expected: %v\n Received: %v\n", a.Key, b.Key)
	}
	if !reflect.DeepEqual(a.Data, b.Data) {
		return false, fmt.Errorf("Data mismatch: Expected Data: %v\n Received Data %v\n", a, b)
	}
	return true, nil
}

func containsTransactions(t *testing.T, list []*apitypes.Transaction, a *apitypes.Transaction) {
	for _, txn := range list {
		if equal, _ := transactionsEqual(txn, a); equal {
			return
		}
	}
	t.Fatalf("Transaction %v not found", a)
}

func verifyTransactions(t *testing.T, logger *PostgresTransactionLogger, list []*apitypes.Transaction) {
	transactChan, errorChan := logger.ReadTransactions()
	transactCount := 0
	for {
		select {
		case transact := <-transactChan:
			containsTransactions(t, list, transact)
			transactCount++
			if transactCount == len(list) {
				return
			}
		case err := <-errorChan:
			if err != io.EOF && err != nil {
				t.Fatalf("Error while reading %v", err)
			}
		case <-time.After(3000 * time.Millisecond):
			t.Fatal("Timeout, no transactions received")
		}
	}
}

func TestIntTransactions(t *testing.T) {
	logger, err := NewPostgresTransactionLogger(postgresUrl)
	if err != nil {
		t.Fatalf("Failed to create postgres logger %v", err)
	}
	defer logger.Close()
	_ = logger.HandleTransactions()

	list := make([]*apitypes.Transaction, 0)
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageInt, TxnType: apitypes.TransactionPut, Key: "testintkey", Data: int32(897734)})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageInt, TxnType: apitypes.TransactionGet, Key: "testintkey"})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageInt, TxnType: apitypes.TransactionPut, Key: "testintkey", Data: int32(-7787)})

	for _, txn := range list {
		logger.WriteTransaction(txn)
	}

	// wait a bit before reading
	<-time.After(300 * time.Millisecond)
	verifyTransactions(t, logger, list)

	logger.WriteTransaction(&apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageInt, TxnType: apitypes.TransactionDel, Key: "testintkey"})
}

func TestFloatTransactions(t *testing.T) {
	logger, err := NewPostgresTransactionLogger(postgresUrl)
	if err != nil {
		t.Fatalf("Failed to create postgres logger %v", err)
	}
	defer logger.Close()
	_ = logger.HandleTransactions()

	list := make([]*apitypes.Transaction, 0)
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageFloat, TxnType: apitypes.TransactionPut, Key: "testfloatkey", Data: float32(3.141592653)})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageFloat, TxnType: apitypes.TransactionGet, Key: "testfloatkey"})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageFloat, TxnType: apitypes.TransactionGet, Key: "testfloatkey"})

	for _, txn := range list {
		logger.WriteTransaction(txn)
	}

	// wait a bit before reading
	<-time.After(3000 * time.Millisecond)
	verifyTransactions(t, logger, list)

	logger.WriteTransaction(&apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageFloat, TxnType: apitypes.TransactionDel, Key: "testfloatkey"})
}

func TestStringTransactions(t *testing.T) {
	logger, err := NewPostgresTransactionLogger(postgresUrl)
	if err != nil {
		t.Fatalf("Failed to create postgres logger %v", err)
	}
	defer logger.Close()
	_ = logger.HandleTransactions()

	list := make([]*apitypes.Transaction, 0)
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageString, TxnType: apitypes.TransactionPut, Key: "teststringkey", Data: "testvalue"})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageString, TxnType: apitypes.TransactionGet, Key: "teststringkey"})
	list = append(list, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageString, TxnType: apitypes.TransactionGet, Key: "teststringkey"})

	for _, txn := range list {
		logger.WriteTransaction(txn)
	}

	// wait a bit before reading
	<-time.After(3000 * time.Millisecond)
	verifyTransactions(t, logger, list)

	logger.WriteTransaction(&apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageString, TxnType: apitypes.TransactionDel, Key: "teststringkey"})
}

// // map transactions
// mapData := map[string]string{"testkey1": "data1", "testkey2": "data2", "testkey3": "data3"}
// transactList = append(transactList, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageMap, TxnType: apitypes.TransactionPut, Key: "testmapkey", Data: mapData})
// transactList = append(transactList, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageMap, TxnType: apitypes.TransactionGet, Key: "testmapkey"})
// transactList = append(transactList, &apitypes.Transaction{Timestamp: time.Now(), StorageType: apitypes.StorageMap, TxnType: apitypes.TransactionDel, Key: "testmapkey"})
