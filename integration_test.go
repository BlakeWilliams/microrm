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
		if err := cleanupTestTables(testDB.db); err != nil {
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

	t.Run("ignores zero value fields during insert", func(t *testing.T) {
		kv := &KeyValue{
			ID:    0,
			Key:   "test.zero.values",
			Value: "",
		}

		err := testDB.Insert(kv)
		require.NoError(t, err)

		require.NotEqual(t, 0, kv.ID, "Zero ID should be ignored and auto-generated")
		require.Greater(t, kv.ID, 0, "Auto-generated ID should be positive")

		// Use sql.NullString to handle potential NULL values
		var retrievedID int
		var retrievedKey string
		var retrievedValue sql.NullString
		row := testDB.db.QueryRow("SELECT id, `key`, value FROM key_values WHERE id = ?", kv.ID)
		err = row.Scan(&retrievedID, &retrievedKey, &retrievedValue)
		require.NoError(t, err)

		require.Equal(t, kv.ID, retrievedID)
		require.Equal(t, "test.zero.values", retrievedKey)

		require.False(t, retrievedValue.Valid, "Value should be NULL in database")
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
