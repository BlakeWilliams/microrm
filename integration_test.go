package microrm

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var testDB *DB

type KeyValue struct {
	ID    int    `db:"id"`
	Key   string `db:"key"`
	Value string `db:"value"`
}

func TestMain(m *testing.M) {
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnv("MYSQL_PORT", "3306")
	user := getEnv("MYSQL_USER", "root")
	password := getEnv("MYSQL_PASSWORD", "")
	database := getEnv("MYSQL_DATABASE", "microrm_test")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true", user, password, host, port, database)
	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/",
		user, password, host, port)

	if err := setupTestDatabase(rootDSN, dsn, database); err != nil {
		log.Printf("Failed to setup test database: %v", err)
		os.Exit(1)
	}

	code := m.Run()

	if testDB != nil {
		if err := cleanupTestTables(testDB.db.(*sql.DB)); err != nil {
			log.Printf("Warning: Failed to cleanup test tables: %v", err)
		}
		testDB.Close()
	}

	os.Exit(code)
}

func TestSelect(t *testing.T) {
	t.Run("select single key-value", func(t *testing.T) {
		var kv KeyValue
		err := testDB.Select(&kv, "WHERE `key` = $key", map[string]any{
			"key": "config.app.name",
		})

		require.NoError(t, err)

		expectedKV := KeyValue{
			ID:    3,
			Key:   "config.app.name",
			Value: "MicroORM",
		}
		require.Equal(t, expectedKV, kv)
	})

	t.Run("select multiple key-values", func(t *testing.T) {
		var kvs []KeyValue
		err := testDB.Select(&kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)

		expectedKVs := []KeyValue{
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
		}
		require.Equal(t, expectedKVs, kvs)
	})

	t.Run("select all key-values", func(t *testing.T) {
		var kvs []KeyValue
		err := testDB.Select(&kvs, "ORDER BY `key`", map[string]any{})

		require.NoError(t, err)

		// This should fail until Select is implemented
		expectedKVs := []KeyValue{
			{ID: 3, Key: "config.app.name", Value: "MicroORM"},
			{ID: 4, Key: "config.app.version", Value: "1.0.0"},
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
			{ID: 5, Key: "feature.cache.enabled", Value: "true"},
		}
		require.Equal(t, expectedKVs, kvs)
	})

	t.Run("select non-existent key", func(t *testing.T) {
		var kv KeyValue
		err := testDB.Select(&kv, "WHERE `key` = $key", map[string]any{
			"key": "non.existent.key",
		})

		require.Error(t, err, sql.ErrNoRows)
		require.Equal(t, KeyValue{}, kv)
	})
}

func TestInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("populates ID of inserted structs", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.insert.key",
			Value: "test insert value",
		}

		require.Equal(t, 0, kv.ID)

		err := testDB.Insert(kv)
		require.NoError(t, err)

		require.NotEqual(t, 0, kv.ID, "ID should be populated after insert")
		require.Greater(t, kv.ID, 0, "ID should be positive")
	})

	t.Run("inserts records with pre-populated IDs", func(t *testing.T) {
		kv := &KeyValue{
			ID:    999,
			Key:   "test.predefined.id",
			Value: "predefined ID value",
		}

		err := testDB.Insert(kv)
		require.NoError(t, err)

		require.Equal(t, 999, kv.ID, "Pre-existing ID should be preserved")
	})

	t.Run("can insert data", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.database.verification",
			Value: "thetruthisoutthere",
		}

		err := testDB.Insert(kv)
		require.NoError(t, err)

		var retrievedKV KeyValue
		row := testDB.db.QueryRow("SELECT id, `key`, value FROM key_values WHERE id = ?", kv.ID)
		err = row.Scan(&retrievedKV.ID, &retrievedKV.Key, &retrievedKV.Value)
		require.NoError(t, err)

		require.Equal(t, kv.ID, retrievedKV.ID)
		require.Equal(t, kv.Key, retrievedKV.Key)
		require.Equal(t, kv.Value, retrievedKV.Value)
	})
}

func TestTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("successful transaction commits changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := testDB.Transaction(func(tx *DB) error {
			kv := &KeyValue{
				Key:   "test.transaction.commit",
				Value: "transaction commit test",
			}

			err := tx.Insert(kv)
			if err != nil {
				return err
			}

			insertedKV = kv
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, insertedKV)
		require.NotEqual(t, 0, insertedKV.ID)

		// Verify the data was committed to the database
		var retrievedKV KeyValue
		err = testDB.Select(&retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.commit",
		})
		require.NoError(t, err)
		require.Equal(t, insertedKV.ID, retrievedKV.ID)
		require.Equal(t, "test.transaction.commit", retrievedKV.Key)
		require.Equal(t, "transaction commit test", retrievedKV.Value)
	})

	t.Run("failed transaction rolls back changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := testDB.Transaction(func(tx *DB) error {
			kv := &KeyValue{
				Key:   "test.transaction.rollback",
				Value: "transaction rollback test",
			}

			err := tx.Insert(kv)
			if err != nil {
				return err
			}

			insertedKV = kv

			// Force an error to trigger rollback
			return fmt.Errorf("intentional error to trigger rollback")
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "intentional error to trigger rollback")
		require.NotNil(t, insertedKV)
		require.NotEqual(t, 0, insertedKV.ID) // ID was set during transaction

		// Verify the data was NOT committed to the database (rolled back)
		var retrievedKV KeyValue
		err = testDB.Select(&retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.rollback",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("multiple operations in transaction", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := testDB.Transaction(func(tx *DB) error {
			// Insert first record
			kv1 = &KeyValue{
				Key:   "test.transaction.multi.1",
				Value: "first record",
			}
			err := tx.Insert(kv1)
			if err != nil {
				return err
			}

			// Insert second record
			kv2 = &KeyValue{
				Key:   "test.transaction.multi.2",
				Value: "second record",
			}
			err = tx.Insert(kv2)
			if err != nil {
				return err
			}

			// Verify we can select within the transaction
			var kvs []KeyValue
			err = tx.Select(&kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
				"pattern": "test.transaction.multi.%",
			})
			if err != nil {
				return err
			}

			if len(kvs) != 2 {
				return fmt.Errorf("expected 2 records in transaction, got %d", len(kvs))
			}

			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, kv1)
		require.NotNil(t, kv2)
		require.NotEqual(t, 0, kv1.ID)
		require.NotEqual(t, 0, kv2.ID)
		require.NotEqual(t, kv1.ID, kv2.ID)

		// Verify both records were committed
		var kvs []KeyValue
		err = testDB.Select(&kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "test.transaction.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		require.Equal(t, "test.transaction.multi.1", kvs[0].Key)
		require.Equal(t, "test.transaction.multi.2", kvs[1].Key)
	})

	t.Run("transaction rollback with multiple operations", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := testDB.Transaction(func(tx *DB) error {
			// Insert first record
			kv1 = &KeyValue{
				Key:   "test.transaction.rollback.multi.1",
				Value: "first record",
			}
			err := tx.Insert(kv1)
			if err != nil {
				return err
			}

			// Insert second record
			kv2 = &KeyValue{
				Key:   "test.transaction.rollback.multi.2",
				Value: "second record",
			}
			err = tx.Insert(kv2)
			if err != nil {
				return err
			}

			// Force rollback after both inserts
			return fmt.Errorf("rollback both operations")
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "rollback both operations")

		// Verify neither record was committed
		var kvs []KeyValue
		err = testDB.Select(&kvs, "WHERE `key` LIKE $pattern", map[string]any{
			"pattern": "test.transaction.rollback.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 0, "No records should be committed after rollback")
	})

	t.Run("transaction with panic triggers rollback", func(t *testing.T) {
		var insertedKV *KeyValue

		// Capture the panic and verify rollback occurred
		func() {
			defer func() {
				if r := recover(); r != nil {
					require.Equal(t, "panic in transaction", r)
				}
			}()

			testDB.Transaction(func(tx *DB) error {
				kv := &KeyValue{
					Key:   "test.transaction.panic",
					Value: "panic test",
				}

				err := tx.Insert(kv)
				if err != nil {
					return err
				}

				insertedKV = kv

				// Trigger panic
				panic("panic in transaction")
			})

			// This should not be reached due to panic
			t.Fatal("Expected panic but transaction completed normally")
		}()

		require.NotNil(t, insertedKV)

		// Verify the data was NOT committed due to panic rollback
		var retrievedKV KeyValue
		err := testDB.Select(&retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.panic",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("nested transactions not supported", func(t *testing.T) {
		err := testDB.Transaction(func(tx *DB) error {
			// Try to start another transaction within an existing one
			return tx.Transaction(func(nestedTx *DB) error {
				return nil
			})
		})

		// This should fail since we're trying to begin a transaction on a *sql.Tx
		require.Error(t, err)
	})
}

func setupTestDatabase(rootDSN, dsn, database string) error {
	rootDB, err := sql.Open("mysql", rootDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer rootDB.Close()

	_, err = rootDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to test database: %w", err)
	}

	testDB = New(db)
	testDB.MapNameToTable("KeyValue", "key_values")

	if err = setupTestTables(db); err != nil {
		testDB.Close()
		return fmt.Errorf("failed to setup test tables: %w", err)
	}

	if err = insertTestData(db); err != nil {
		testDB.Close()
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	return nil
}

func setupTestTables(db *sql.DB) error {
	dropSQL := `DROP TABLE IF EXISTS key_values;`
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}
	createSQL := `
		CREATE TABLE key_values (
			id INT AUTO_INCREMENT PRIMARY KEY,
			` + "`key`" + ` VARCHAR(255) NOT NULL UNIQUE,
			value TEXT NULL
		)
	`

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create key_values table: %w", err)
	}

	return nil
}

func insertTestData(db *sql.DB) error {
	keyValueData := []struct {
		key, value string
	}{
		{"config.database.host", "localhost"},
		{"config.database.port", "3306"},
		{"config.app.name", "MicroORM"},
		{"config.app.version", "1.0.0"},
		{"feature.cache.enabled", "true"},
	}

	for _, kv := range keyValueData {
		_, err := db.Exec("INSERT INTO key_values (`key`, value) VALUES (?, ?)", kv.key, kv.value)
		if err != nil {
			return fmt.Errorf("failed to insert key-value data: %w", err)
		}
	}

	return nil
}

func cleanupTestTables(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS key_values")
	return err
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
