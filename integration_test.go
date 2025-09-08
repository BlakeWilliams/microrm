package dbmap

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

type mockClock struct {
	currentTime time.Time
}

func (m *mockClock) Now() time.Time {
	return m.currentTime
}

func (m *mockClock) Advance(d time.Duration) {
	m.currentTime = m.currentTime.Add(d)
}

func newMockClock(t time.Time) *mockClock {
	return &mockClock{currentTime: t}
}

type KeyValue struct {
	ID        int       `db:"id"`
	Key       string    `db:"key"`
	Value     string    `db:"value"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

type CustomIDStruct struct {
	CustomID int    `db:"id"`
	Key      string `db:"key"`
	Value    string `db:"value"`
}

func (c *CustomIDStruct) TableName() string {
	return "key_values"
}

type NullTimeKeyValue struct {
	ID        int          `db:"id"`
	Key       string       `db:"key"`
	Value     string       `db:"value"`
	CreatedAt sql.NullTime `db:"created_at"`
	UpdatedAt sql.NullTime `db:"updated_at"`
}

func (n *NullTimeKeyValue) TableName() string {
	return "key_values"
}

func setupDB(t *testing.T) *sql.DB {
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnv("MYSQL_PORT", "3306")
	user := getEnv("MYSQL_USER", "root")
	password := getEnv("MYSQL_PASSWORD", "")
	database := getEnv("MYSQL_DATABASE", "dbmap_test")

	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, password, host, port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true", user, password, host, port, database)

	rootDB, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer rootDB.Close()

	_, err = rootDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	if err = setupTestTables(sqlDB); err != nil {
		sqlDB.Close()
		t.Fatalf("Failed to setup test tables: %v", err)
	}

	if err = insertTestData(sqlDB); err != nil {
		sqlDB.Close()
		t.Fatalf("Failed to insert test data: %v", err)
	}

	t.Cleanup(func() {
		if err := truncateTestTables(sqlDB); err != nil {
			t.Logf("Warning: Failed to truncate test tables: %v", err)
		}
		sqlDB.Close()
	})

	return sqlDB
}

func TestSelect(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("select single key-value", func(t *testing.T) {
		var kv KeyValue
		err := db.Select(ctx, &kv, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.NoError(t, err)

		expectedKV := KeyValue{
			ID:    3,
			Key:   "config.app.name",
			Value: "MicroORM",
		}
		requireKVEqual(t, expectedKV, kv)
	})

	t.Run("select multiple key-values", func(t *testing.T) {
		var kvs []KeyValue
		err := db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)

		expectedKVs := []KeyValue{
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
		}
		requireKVsEqual(t, expectedKVs, kvs)
	})

	t.Run("select multiple key-values into slice of pointers", func(t *testing.T) {
		var kvs []*KeyValue
		err := db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)
		require.Len(t, kvs, 2)

		expectedKVs := []*KeyValue{
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
		}

		requireKVsEqual(t, expectedKVs, kvs)
	})

	t.Run("select all key-values", func(t *testing.T) {
		var kvs []KeyValue
		err := db.Select(ctx, &kvs, "ORDER BY `key`", Args{})

		require.NoError(t, err)

		expectedKVs := []KeyValue{
			{ID: 3, Key: "config.app.name", Value: "MicroORM"},
			{ID: 4, Key: "config.app.version", Value: "1.0.0"},
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
			{ID: 5, Key: "feature.cache.enabled", Value: "true"},
		}
		requireKVsEqual(t, expectedKVs, kvs)
	})

	t.Run("select non-existent key", func(t *testing.T) {
		var kv KeyValue
		err := db.Select(ctx, &kv, "WHERE `key` = $key", Args{
			"key": "non.existent.key",
		})

		require.Error(t, err, sql.ErrNoRows)
		requireKVEqual(t, KeyValue{}, kv)
	})
}

func TestInsert(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("populates ID of inserted structs", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.insert.key",
			Value: "test insert value",
		}

		require.Equal(t, 0, kv.ID)

		err := db.Insert(ctx, kv)
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

		err := db.Insert(ctx, kv)
		require.NoError(t, err)

		require.Equal(t, 999, kv.ID, "Pre-existing ID should be preserved")
	})

	t.Run("can insert data", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.database.verification",
			Value: "thetruthisoutthere",
		}

		err := db.Insert(ctx, kv)
		require.NoError(t, err)

		var retrievedKV KeyValue
		row := db.db.QueryRowContext(ctx, "SELECT id, `key`, value FROM key_values WHERE id = ?", kv.ID)
		err = row.Scan(&retrievedKV.ID, &retrievedKV.Key, &retrievedKV.Value)
		require.NoError(t, err)

		require.Equal(t, kv.ID, retrievedKV.ID)
		require.Equal(t, kv.Key, retrievedKV.Key)
		require.Equal(t, kv.Value, retrievedKV.Value)
	})

	t.Run("insert automatically sets CreatedAt field", func(t *testing.T) {
		insertTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		mockClock := newMockClock(insertTime)

		db := &DB{
			db:             db.db,
			modelTypeCache: db.modelTypeCache,
			Pluralizer:     db.Pluralizer,
			time:           mockClock,
		}

		kv := &KeyValue{
			Key:   "test.insert.createdat",
			Value: "test created at",
		}

		require.True(t, kv.CreatedAt.IsZero(), "CreatedAt should be zero before insert")

		err := db.Insert(ctx, kv)
		require.NoError(t, err)

		require.False(t, kv.CreatedAt.IsZero(), "CreatedAt should not be zero after insert")
		require.Equal(t, insertTime, kv.CreatedAt, "CreatedAt should match the mock time")
		require.Equal(t, insertTime, kv.UpdatedAt, "UpdatedAt should match the mock time")

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE `key` = $key", Args{"key": "test.insert.createdat"})
		require.NoError(t, err)
		require.WithinDuration(t, kv.CreatedAt, retrievedKV.CreatedAt, time.Second, "CreatedAt should match between struct and database within 1 second")
	})
}

func TestTransaction(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("successful transaction commits changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := db.Transaction(ctx, func(tx *DB) error {
			kv := &KeyValue{
				Key:   "test.transaction.commit",
				Value: "transaction commit test",
			}

			err := tx.Insert(ctx, kv)
			if err != nil {
				return err
			}

			insertedKV = kv
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, insertedKV)
		require.NotEqual(t, 0, insertedKV.ID)

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE `key` = $key", Args{
			"key": "test.transaction.commit",
		})
		require.NoError(t, err)
		require.Equal(t, insertedKV.ID, retrievedKV.ID)
		require.Equal(t, "test.transaction.commit", retrievedKV.Key)
		require.Equal(t, "transaction commit test", retrievedKV.Value)
	})

	t.Run("failed transaction rolls back changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := db.Transaction(ctx, func(tx *DB) error {
			kv := &KeyValue{
				Key:   "test.transaction.rollback",
				Value: "transaction rollback test",
			}

			err := tx.Insert(ctx, kv)
			if err != nil {
				return err
			}

			insertedKV = kv

			return fmt.Errorf("intentional error to trigger rollback")
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "intentional error to trigger rollback")
		require.NotNil(t, insertedKV)
		require.NotEqual(t, 0, insertedKV.ID)

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE `key` = $key", Args{
			"key": "test.transaction.rollback",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("multiple operations in transaction", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := db.Transaction(ctx, func(tx *DB) error {
			kv1 = &KeyValue{
				Key:   "test.transaction.multi.1",
				Value: "first record",
			}
			err := tx.Insert(ctx, kv1)
			if err != nil {
				return err
			}

			kv2 = &KeyValue{
				Key:   "test.transaction.multi.2",
				Value: "second record",
			}
			err = tx.Insert(ctx, kv2)
			if err != nil {
				return err
			}

			var kvs []KeyValue
			err = tx.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
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

		var kvs []KeyValue
		err = db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "test.transaction.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		require.Equal(t, "test.transaction.multi.1", kvs[0].Key)
		require.Equal(t, "test.transaction.multi.2", kvs[1].Key)
	})

	t.Run("transaction rollback with multiple operations", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := db.Transaction(ctx, func(tx *DB) error {
			kv1 = &KeyValue{
				Key:   "test.transaction.rollback.multi.1",
				Value: "first record",
			}
			err := tx.Insert(ctx, kv1)
			if err != nil {
				return err
			}

			kv2 = &KeyValue{
				Key:   "test.transaction.rollback.multi.2",
				Value: "second record",
			}
			err = tx.Insert(ctx, kv2)
			if err != nil {
				return err
			}

			return fmt.Errorf("rollback both operations")
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "rollback both operations")

		var kvs []KeyValue
		err = db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.transaction.rollback.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 0, "No records should be committed after rollback")
	})

	t.Run("transaction with panic triggers rollback", func(t *testing.T) {
		var insertedKV *KeyValue

		func() {
			defer func() {
				if r := recover(); r != nil {
					require.Equal(t, "panic in transaction", r)
				}
			}()

			_ = db.Transaction(ctx, func(tx *DB) error {
				kv := &KeyValue{
					Key:   "test.transaction.panic",
					Value: "panic test",
				}

				err := tx.Insert(ctx, kv)
				if err != nil {
					return err
				}

				insertedKV = kv

				panic("panic in transaction")
			})

			t.Fatal("Expected panic but transaction completed normally")
		}()

		require.NotNil(t, insertedKV)

		var retrievedKV KeyValue
		err := db.Select(ctx, &retrievedKV, "WHERE `key` = $key", Args{
			"key": "test.transaction.panic",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("nested transactions not supported", func(t *testing.T) {
		err := db.Transaction(ctx, func(tx *DB) error {
			return tx.Transaction(ctx, func(nestedTx *DB) error {
				return nil
			})
		})

		require.Error(t, err)
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("delete single record by key", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.delete.single",
			Value: "to be deleted",
		}
		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE `key` = $key", Args{
			"key": "test.delete.single",
		})
		require.NoError(t, err)

		rowsAffected, err := db.Delete(ctx, &KeyValue{}, "WHERE `key` = $key", Args{
			"key": "test.delete.single",
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = db.Select(ctx, &deletedKV, "WHERE `key` = $key", Args{
			"key": "test.delete.single",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("delete multiple records with pattern", func(t *testing.T) {
		testRecords := []KeyValue{
			{Key: "test.delete.multi.1", Value: "first"},
			{Key: "test.delete.multi.2", Value: "second"},
			{Key: "test.delete.multi.3", Value: "third"},
		}

		for i := range testRecords {
			err := db.Insert(ctx, &testRecords[i])
			require.NoError(t, err)
		}

		var kvs []KeyValue
		err := db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "test.delete.multi.%",
		})

		require.NoError(t, err)
		require.Len(t, kvs, 3)

		rowsAffected, err := db.Delete(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.delete.multi.%",
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), rowsAffected)

		var deletedKVs []KeyValue
		err = db.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.delete.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, deletedKVs, 0)
	})

	t.Run("delete by ID", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.delete.by.id",
			Value: "delete by ID test",
		}
		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)
		insertedID := kv.ID

		rowsAffected, err := db.Delete(ctx, &KeyValue{}, "WHERE id = $id", Args{
			"id": insertedID,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = db.Select(ctx, &deletedKV, "WHERE id = $id", Args{
			"id": insertedID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("delete non-existent record returns zero rows affected", func(t *testing.T) {
		rowsAffected, err := db.Delete(ctx, &KeyValue{}, "WHERE `key` = $key", Args{
			"key": "non.existent.key.for.delete",
		})
		require.NoError(t, err)
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete with complex WHERE clause", func(t *testing.T) {
		testRecords := []KeyValue{
			{Key: "test.delete.complex.keep", Value: "keep this"},
			{Key: "test.delete.complex.remove1", Value: "remove this"},
			{Key: "test.delete.complex.remove2", Value: "remove this too"},
			{Key: "test.delete.complex.keep2", Value: "keep this too"},
		}

		for i := range testRecords {
			err := db.Insert(ctx, &testRecords[i])
			require.NoError(t, err)
		}

		rowsAffected, err := db.Delete(ctx, &KeyValue{}, "WHERE `key` LIKE $keyPattern AND `value` LIKE $valuePattern", Args{
			"keyPattern":   "test.delete.complex.remove%",
			"valuePattern": "%remove%",
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var remainingKVs []KeyValue
		err = db.Select(ctx, &remainingKVs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "test.delete.complex.%",
		})
		require.NoError(t, err)
		require.Len(t, remainingKVs, 2)
		require.Equal(t, "test.delete.complex.keep", remainingKVs[0].Key)
		require.Equal(t, "test.delete.complex.keep2", remainingKVs[1].Key)
	})
}

func TestDeleteRecord(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("delete record by ID field", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.deleterecord.basic",
			Value: "basic delete test",
		}
		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)
		insertedID := kv.ID

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE id = $id", Args{
			"id": insertedID,
		})
		require.NoError(t, err)
		require.Equal(t, insertedID, retrievedKV.ID)

		rowsAffected, err := db.DeleteRecord(ctx, kv)
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = db.Select(ctx, &deletedKV, "WHERE id = $id", Args{
			"id": insertedID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("delete record without ID field should error", func(t *testing.T) {
		type NoIDStruct struct {
			Key   string `db:"key"`
			Value string `db:"value"`
		}

		noID := &NoIDStruct{
			Key:   "test.no.id",
			Value: "no ID field",
		}

		rowsAffected, err := db.DeleteRecord(ctx, noID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct does not have an ID field")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete record with custom db tag for ID", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.deleterecord.customid",
			Value: "custom ID test",
		}
		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)

		customKV := &CustomIDStruct{
			CustomID: kv.ID,
			Key:      "test.deleterecord.customid",
			Value:    "custom ID test",
		}

		rowsAffected, err := db.DeleteRecord(ctx, customKV)
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = db.Select(ctx, &deletedKV, "WHERE id = $id", Args{
			"id": kv.ID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})
}

func TestDeleteRecords(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("delete multiple records by slice", func(t *testing.T) {
		testRecords := []*KeyValue{
			{Key: "test.deleterecords.1", Value: "first record"},
			{Key: "test.deleterecords.2", Value: "second record"},
			{Key: "test.deleterecords.3", Value: "third record"},
		}

		for _, kv := range testRecords {
			err := db.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		var kvs []KeyValue
		err := db.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "test.deleterecords.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 3)

		rowsAffected, err := db.DeleteRecords(ctx, testRecords)
		require.NoError(t, err)
		require.Equal(t, int64(3), rowsAffected)

		var deletedKVs []KeyValue
		err = db.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.deleterecords.%",
		})
		require.NoError(t, err)
		require.Len(t, deletedKVs, 0)
	})

	t.Run("delete records by pointer to slice", func(t *testing.T) {
		testRecords := []*KeyValue{
			{Key: "test.deleterecords.ptr.1", Value: "first record"},
			{Key: "test.deleterecords.ptr.2", Value: "second record"},
		}

		for _, kv := range testRecords {
			err := db.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		rowsAffected, err := db.DeleteRecords(ctx, &testRecords)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var deletedKVs []KeyValue
		err = db.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.deleterecords.ptr.%",
		})
		require.NoError(t, err)
		require.Len(t, deletedKVs, 0)
	})

	t.Run("delete records by slice of values", func(t *testing.T) {
		testRecordPtrs := []*KeyValue{
			{Key: "test.deleterecords.values.1", Value: "first record"},
			{Key: "test.deleterecords.values.2", Value: "second record"},
		}

		for _, kv := range testRecordPtrs {
			err := db.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		testRecordValues := []KeyValue{
			*testRecordPtrs[0],
			*testRecordPtrs[1],
		}

		rowsAffected, err := db.DeleteRecords(ctx, testRecordValues)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var deletedKVs []KeyValue
		err = db.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", Args{
			"pattern": "test.deleterecords.values.%",
		})
		require.NoError(t, err)
		require.Len(t, deletedKVs, 0)
	})

	t.Run("delete empty slice returns zero rows affected", func(t *testing.T) {
		emptyRecords := []*KeyValue{}

		rowsAffected, err := db.DeleteRecords(ctx, emptyRecords)
		require.NoError(t, err)
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete records without ID field should error", func(t *testing.T) {
		type NoIDStruct struct {
			Key   string `db:"key"`
			Value string `db:"value"`
		}

		noIDRecords := []*NoIDStruct{
			{Key: "test.no.id.1", Value: "no ID field 1"},
			{Key: "test.no.id.2", Value: "no ID field 2"},
		}

		rowsAffected, err := db.DeleteRecords(ctx, noIDRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct does not have an ID field")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete records with mixed success and failure should rollback", func(t *testing.T) {
		validKV := &KeyValue{
			Key:   "test.deleterecords.rollback.valid",
			Value: "valid record",
		}
		err := db.Insert(ctx, validKV)
		require.NoError(t, err)
		require.NotEqual(t, 0, validKV.ID)

		type NoIDStruct struct {
			Key   string `db:"key"`
			Value string `db:"value"`
		}

		mixedRecords := []any{
			validKV,
			&NoIDStruct{Key: "test.no.id", Value: "no ID field"},
		}

		rowsAffected, err := db.DeleteRecords(ctx, mixedRecords)
		require.Error(t, err)
		require.Equal(t, int64(0), rowsAffected)

		var retrievedKV KeyValue
		err = db.Select(ctx, &retrievedKV, "WHERE id = $id", Args{
			"id": validKV.ID,
		})
		require.NoError(t, err)
		require.Equal(t, validKV.ID, retrievedKV.ID)
	})

	t.Run("delete with non-slice parameter should error", func(t *testing.T) {
		singleRecord := &KeyValue{
			Key:   "test.single.record",
			Value: "single record",
		}

		rowsAffected, err := db.DeleteRecords(ctx, singleRecord)
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination must be a slice")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete records with custom db tag for ID", func(t *testing.T) {
		testKVs := []*KeyValue{
			{Key: "test.deleterecords.customid.1", Value: "custom ID test 1"},
			{Key: "test.deleterecords.customid.2", Value: "custom ID test 2"},
		}

		for _, kv := range testKVs {
			err := db.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		customRecords := []*CustomIDStruct{
			{CustomID: testKVs[0].ID, Key: "test.deleterecords.customid.1", Value: "custom ID test 1"},
			{CustomID: testKVs[1].ID, Key: "test.deleterecords.customid.2", Value: "custom ID test 2"},
		}

		rowsAffected, err := db.DeleteRecords(ctx, customRecords)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		for _, kv := range testKVs {
			var deletedKV KeyValue
			err = db.Select(ctx, &deletedKV, "WHERE id = $id", Args{
				"id": kv.ID,
			})
			require.Error(t, err)
			require.Equal(t, sql.ErrNoRows, err)
		}
	})
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("update single column", func(t *testing.T) {
		orig := &KeyValue{Key: "test.update.single", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)

		rows, err := db.Update(ctx, &KeyValue{}, "WHERE `key` = $key", Args{"key": "test.update.single"}, Updates{"Value": "after"})
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		var kv KeyValue
		err = db.Select(ctx, &kv, "WHERE `key` = $key", Args{"key": "test.update.single"})
		require.NoError(t, err)
		require.Equal(t, "after", kv.Value)
	})

	t.Run("update multiple columns including key", func(t *testing.T) {
		orig := &KeyValue{Key: "test.update.multi.orig", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)

		rows, err := db.Update(ctx, &KeyValue{}, "WHERE `key` = $key", Args{"key": "test.update.multi.orig"}, Updates{"Key": "test.update.multi.new", "Value": "after"})
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		var oldKV KeyValue
		err = db.Select(ctx, &oldKV, "WHERE `key` = $key", Args{"key": "test.update.multi.orig"})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)

		var newKV KeyValue
		err = db.Select(ctx, &newKV, "WHERE `key` = $key", Args{"key": "test.update.multi.new"})
		require.NoError(t, err)
		require.Equal(t, "after", newKV.Value)
	})

	t.Run("update with no matching row returns zero", func(t *testing.T) {
		rows, err := db.Update(ctx, &KeyValue{}, "WHERE `key` = $key", Args{"key": "does.not.exist.update"}, Updates{"Value": "whatever"})
		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})

	t.Run("update with invalid column returns error", func(t *testing.T) {
		t.Skip("TODO")
		orig := &KeyValue{Key: "test.update.invalidcol", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))

		_, err := db.Update(ctx, &KeyValue{}, "WHERE `key` = $key", Args{"key": "test.update.invalidcol"}, Updates{"not_a_column": "x"})
		require.Error(t, err)
	})

	t.Run("update automatically sets UpdatedAt field", func(t *testing.T) {
		orig := &KeyValue{Key: "test.update.updatedat", Value: "original"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)

		pastTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		_, err := db.db.ExecContext(ctx, "UPDATE key_values SET updated_at = ? WHERE id = ?", pastTime, orig.ID)
		require.NoError(t, err)

		var kvBefore KeyValue
		err = db.Select(ctx, &kvBefore, "WHERE id = $id", Args{"id": orig.ID})
		require.NoError(t, err)
		require.True(t, kvBefore.UpdatedAt.Equal(pastTime))

		updateTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		mockClock := newMockClock(updateTime)

		db := &DB{
			db:             db.db,
			modelTypeCache: db.modelTypeCache,
			Pluralizer:     db.Pluralizer,
			time:           mockClock,
		}

		rows, err := db.Update(ctx, &KeyValue{}, "WHERE `key` = $key", Args{"key": "test.update.updatedat"}, Updates{"Value": "updated"})
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		var kv KeyValue
		err = db.Select(ctx, &kv, "WHERE `key` = $key", Args{"key": "test.update.updatedat"})
		require.NoError(t, err)
		require.Equal(t, "updated", kv.Value)

		require.WithinDuration(t, updateTime, kv.UpdatedAt, time.Second, "UpdatedAt should match the mock time within 1 second")
		require.True(t, kv.UpdatedAt.After(pastTime), "UpdatedAt should be much later than the manually set past time")
	})
}

func TestUpdateRecord(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("update single column by ID", func(t *testing.T) {
		orig := &KeyValue{Key: "test.updaterecord.single", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)
		originalID := orig.ID

		err := db.UpdateRecord(ctx, orig, Updates{"Value": "after"})
		require.NoError(t, err)

		require.Equal(t, "after", orig.Value)
		require.Equal(t, originalID, orig.ID)

		var kv KeyValue
		err = db.Select(ctx, &kv, "WHERE id = $id", Args{"id": originalID})
		require.NoError(t, err)
		require.Equal(t, "after", kv.Value)
		require.Equal(t, "test.updaterecord.single", kv.Key)
		require.Equal(t, originalID, kv.ID)
	})

	t.Run("update multiple columns by ID", func(t *testing.T) {
		orig := &KeyValue{Key: "test.updaterecord.multi", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)
		originalID := orig.ID

		err := db.UpdateRecord(ctx, orig, Updates{"Key": "test.updaterecord.multi.new", "Value": "after"})
		require.NoError(t, err)

		require.Equal(t, "test.updaterecord.multi.new", orig.Key)
		require.Equal(t, "after", orig.Value)
		require.Equal(t, originalID, orig.ID)

		var kv KeyValue
		err = db.Select(ctx, &kv, "WHERE id = $id", Args{"id": originalID})
		require.NoError(t, err)
		require.Equal(t, "test.updaterecord.multi.new", kv.Key)
		require.Equal(t, "after", kv.Value)
		require.Equal(t, originalID, kv.ID)
	})

	t.Run("update with no changes returns success", func(t *testing.T) {
		orig := &KeyValue{Key: "test.updaterecord.nochange", Value: "unchanged"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)

		err := db.UpdateRecord(ctx, orig, Updates{"Value": "unchanged"})
		require.NoError(t, err)
	})

	t.Run("update record without ID field should error", func(t *testing.T) {
		type NoIDStruct struct {
			Key   string `db:"key"`
			Value string `db:"value"`
		}

		noID := &NoIDStruct{
			Key:   "test.no.id",
			Value: "no ID field",
		}

		err := db.UpdateRecord(ctx, noID, Updates{"Value": "new value"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct does not have an ID field")
	})

	t.Run("update with empty updates should error", func(t *testing.T) {
		orig := &KeyValue{Key: "test.updaterecord.empty", Value: "value"}
		require.NoError(t, db.Insert(ctx, orig))

		err := db.UpdateRecord(ctx, orig, Updates{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no updates provided")
	})

	t.Run("update with invalid column returns error", func(t *testing.T) {
		t.Skip("TODO")
		orig := &KeyValue{Key: "test.updaterecord.invalidcol", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))

		err := db.UpdateRecord(ctx, orig, Updates{"not_a_column": "x"})
		require.Error(t, err)
	})

	t.Run("update non-existent record returns zero rows", func(t *testing.T) {
		nonExistent := &KeyValue{ID: 99999, Key: "fake", Value: "fake"}

		err := db.UpdateRecord(ctx, nonExistent, Updates{"Value": "new value"})
		require.NoError(t, err)
	})

	t.Run("update with custom db tag for ID", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.updaterecord.customid",
			Value: "original value",
		}
		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)

		customKV := &CustomIDStruct{
			CustomID: kv.ID,
			Key:      "test.updaterecord.customid",
			Value:    "original value",
		}

		err = db.UpdateRecord(ctx, customKV, Updates{"Value": "updated value"})
		require.NoError(t, err)

		require.Equal(t, "updated value", customKV.Value)

		var updatedKV KeyValue
		err = db.Select(ctx, &updatedKV, "WHERE id = $id", Args{"id": kv.ID})
		require.NoError(t, err)
		require.Equal(t, "updated value", updatedKV.Value)
	})

	t.Run("update with non-pointer struct should error", func(t *testing.T) {
		kv := KeyValue{ID: 1, Key: "test", Value: "value"}

		err := db.UpdateRecord(ctx, kv, Updates{"Value": "new value"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination must be a pointer to a struct")
	})

	t.Run("update ID field should work", func(t *testing.T) {
		orig := &KeyValue{Key: "test.updaterecord.updateid", Value: "value"}
		require.NoError(t, db.Insert(ctx, orig))
		require.NotZero(t, orig.ID)
		originalID := orig.ID

		newID := originalID + 1000
		err := db.UpdateRecord(ctx, orig, Updates{"ID": newID})
		require.NoError(t, err)

		require.Equal(t, newID, orig.ID)

		var oldKV KeyValue
		err = db.Select(ctx, &oldKV, "WHERE id = $id", Args{"id": originalID})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)

		var newKV KeyValue
		err = db.Select(ctx, &newKV, "WHERE id = $id", Args{"id": newID})
		require.NoError(t, err)
		require.Equal(t, "test.updaterecord.updateid", newKV.Key)
		require.Equal(t, "value", newKV.Value)
	})

	t.Run("update automatically sets UpdatedAt and preserves CreatedAt", func(t *testing.T) {
		initialTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		mockClock := newMockClock(initialTime)

		db := &DB{
			db:             db.db,
			modelTypeCache: db.modelTypeCache,
			Pluralizer:     db.Pluralizer,
			time:           mockClock,
		}

		kv := &KeyValue{
			Key:   "test.updaterecord.timestamps",
			Value: "initial value",
		}

		err := db.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotZero(t, kv.ID)

		originalCreatedAt := kv.CreatedAt
		originalUpdatedAt := kv.UpdatedAt

		require.Equal(t, initialTime, originalCreatedAt)
		require.Equal(t, initialTime, originalUpdatedAt)

		mockClock.Advance(5 * time.Minute)
		updateTime := mockClock.Now()

		err = db.UpdateRecord(ctx, kv, Updates{"Value": "updated value", "Key": "test.updaterecord.timestamps.updated"})
		require.NoError(t, err)

		require.Equal(t, "updated value", kv.Value)
		require.Equal(t, "test.updaterecord.timestamps.updated", kv.Key)
		require.Equal(t, originalCreatedAt, kv.CreatedAt, "CreatedAt should not change during updates")
		require.Equal(t, updateTime, kv.UpdatedAt, "UpdatedAt should match the mock time")
		require.True(t, kv.UpdatedAt.After(originalUpdatedAt), "UpdatedAt should be newer than original")

		var dbKV KeyValue
		err = db.Select(ctx, &dbKV, "WHERE id = $id", Args{"id": kv.ID})
		require.NoError(t, err)
		require.Equal(t, "updated value", dbKV.Value)
		require.Equal(t, "test.updaterecord.timestamps.updated", dbKV.Key)
		require.WithinDuration(t, originalCreatedAt, dbKV.CreatedAt, 2*time.Second, "CreatedAt should be preserved in database")
		require.WithinDuration(t, kv.UpdatedAt, dbKV.UpdatedAt, 2*time.Second, "UpdatedAt should match between struct and database")
	})
}

func TestSqlNullTime(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("insert with automatic timestamp setting", func(t *testing.T) {
		insertTime := time.Date(2024, 2, 1, 10, 0, 0, 0, time.UTC)
		mockClock := newMockClock(insertTime)

		db := &DB{
			db:             db.db,
			modelTypeCache: db.modelTypeCache,
			Pluralizer:     db.Pluralizer,
			time:           mockClock,
		}

		nullTimeKV := &NullTimeKeyValue{
			Key:   "test.sql.nulltime.insert",
			Value: "test value",
		}

		require.False(t, nullTimeKV.CreatedAt.Valid)
		require.False(t, nullTimeKV.UpdatedAt.Valid)

		err := db.Insert(ctx, nullTimeKV)
		require.NoError(t, err)
		require.NotZero(t, nullTimeKV.ID)

		require.True(t, nullTimeKV.CreatedAt.Valid)
		require.True(t, nullTimeKV.UpdatedAt.Valid)

		require.Equal(t, insertTime, nullTimeKV.CreatedAt.Time)
		require.Equal(t, insertTime, nullTimeKV.UpdatedAt.Time)

		var dbKV NullTimeKeyValue
		err = db.Select(ctx, &dbKV, "WHERE id = $id", Args{"id": nullTimeKV.ID})
		require.NoError(t, err)
		require.Equal(t, nullTimeKV.Key, dbKV.Key)
		require.Equal(t, nullTimeKV.Value, dbKV.Value)
		require.True(t, dbKV.CreatedAt.Valid)
		require.True(t, dbKV.UpdatedAt.Valid)
		require.WithinDuration(t, nullTimeKV.CreatedAt.Time, dbKV.CreatedAt.Time, 2*time.Second)
		require.WithinDuration(t, nullTimeKV.UpdatedAt.Time, dbKV.UpdatedAt.Time, 2*time.Second)
	})

	t.Run("update with automatic UpdatedAt setting", func(t *testing.T) {
		insertTime := time.Date(2024, 2, 1, 10, 0, 0, 0, time.UTC)
		mockClock := newMockClock(insertTime)

		db := &DB{
			db:             db.db,
			modelTypeCache: db.modelTypeCache,
			Pluralizer:     db.Pluralizer,
			time:           mockClock,
		}

		nullTimeKV := &NullTimeKeyValue{
			Key:   "test.sql.nulltime.update",
			Value: "initial value",
		}

		err := db.Insert(ctx, nullTimeKV)
		require.NoError(t, err)
		originalID := nullTimeKV.ID
		originalCreatedAt := nullTimeKV.CreatedAt.Time
		originalUpdatedAt := nullTimeKV.UpdatedAt.Time

		mockClock.Advance(5 * time.Minute)
		updateTime := mockClock.Now()

		err = db.UpdateRecord(ctx, nullTimeKV, Updates{"Value": "updated value"})
		require.NoError(t, err)

		require.Equal(t, originalID, nullTimeKV.ID)
		require.Equal(t, "updated value", nullTimeKV.Value)
		require.True(t, nullTimeKV.CreatedAt.Valid)
		require.True(t, nullTimeKV.UpdatedAt.Valid)
		require.WithinDuration(t, originalCreatedAt, nullTimeKV.CreatedAt.Time, time.Second)
		require.Equal(t, updateTime, nullTimeKV.UpdatedAt.Time)
		require.True(t, nullTimeKV.UpdatedAt.Time.After(originalUpdatedAt))
		require.True(t, nullTimeKV.UpdatedAt.Time.After(originalUpdatedAt))

		var dbKV NullTimeKeyValue
		err = db.Select(ctx, &dbKV, "WHERE id = $id", Args{"id": originalID})
		require.NoError(t, err)
		require.Equal(t, "updated value", dbKV.Value)
		require.WithinDuration(t, originalCreatedAt, dbKV.CreatedAt.Time, 2*time.Second)
		require.WithinDuration(t, nullTimeKV.UpdatedAt.Time, dbKV.UpdatedAt.Time, 2*time.Second)
	})
}

func TestQuery(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("query with parameters", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT * FROM key_values WHERE id BETWEEN $min AND $max", map[string]any{
			"min": 1,
			"max": 2,
		})
		require.NoError(t, err)
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}
		require.Equal(t, 2, count)
	})

	t.Run("query with missing parameter", func(t *testing.T) {
		_, err := db.Query(ctx, "SELECT * FROM key_values WHERE id = $missing", map[string]any{
			"other": 1,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing argument for named parameter: missing")
	})

	t.Run("query with escaped dollar sign", func(t *testing.T) {
		rows, err := db.Query(ctx, "SELECT '$$test' as literal_dollar", map[string]any{})
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var result string
		err = rows.Scan(&result)
		require.NoError(t, err)
		require.Equal(t, "$test", result)
	})
}

func TestExec(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("exec insert with named parameters", func(t *testing.T) {
		result, err := db.Exec(ctx, "INSERT INTO key_values (`key`, value) VALUES ($key, $value)", map[string]any{
			"key":   "test.exec.insert",
			"value": "test value",
		})
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		lastID, err := result.LastInsertId()
		require.NoError(t, err)
		require.Greater(t, lastID, int64(0))
	})

	t.Run("exec delete with named parameters", func(t *testing.T) {
		result, err := db.Exec(ctx, "DELETE FROM key_values WHERE `key` = $key", map[string]any{
			"key": "test.exec.insert",
		})
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)
	})

	t.Run("exec with no parameters", func(t *testing.T) {
		result, err := db.Exec(ctx, "INSERT INTO key_values (`key`, value) VALUES ('test.no.params', 'no params')", map[string]any{})
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		_, err = db.Exec(ctx, "DELETE FROM key_values WHERE `key` = 'test.no.params'", map[string]any{})
		require.NoError(t, err)
	})

	t.Run("exec with missing parameter", func(t *testing.T) {
		_, err := db.Exec(ctx, "INSERT INTO key_values (`key`, value) VALUES ($key, $missing)", map[string]any{
			"key": "test.missing",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing argument for named parameter: missing")
	})

	t.Run("exec with escaped dollar sign", func(t *testing.T) {
		result, err := db.Exec(ctx, "INSERT INTO key_values (`key`, value) VALUES ('test.dollar', '$$escaped')", map[string]any{})
		require.NoError(t, err)

		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		_, err = db.Exec(ctx, "DELETE FROM key_values WHERE `key` = 'test.dollar'", map[string]any{})
		require.NoError(t, err)
	})
}

func setupTestTables(db *sql.DB) error {
	// Drop existing tables
	dropSQL := `DROP TABLE IF EXISTS key_values, users;`
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop existing tables: %w", err)
	}

	// Create key_values table for integration tests
	createKeyValuesSQL := `
		CREATE TABLE key_values (
			id INT AUTO_INCREMENT PRIMARY KEY,
			` + "`key`" + ` VARCHAR(255) NOT NULL UNIQUE,
			value TEXT NULL,
			created_at TIMESTAMP NULL,
			updated_at TIMESTAMP NULL
		)
	`
	if _, err := db.Exec(createKeyValuesSQL); err != nil {
		return fmt.Errorf("failed to create key_values table: %w", err)
	}

	// Create users table for examples
	createUsersSQL := `
		CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP NULL,
			updated_at TIMESTAMP NULL
		)
	`
	if _, err := db.Exec(createUsersSQL); err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	return nil
}

func insertTestData(db *sql.DB) error {
	// Insert key-value data for integration tests
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
		_, err := db.Exec("INSERT INTO key_values (`key`, value, created_at, updated_at) VALUES (?, ?, ?, ?)", kv.key, kv.value, time.Now().UTC(), time.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to insert key-value data: %w", err)
		}
	}

	// Insert user data for examples
	userData := []struct {
		name, email string
		active      bool
	}{
		{"Fox Mulder", "mulder@fbi.gov", true},
		{"Dana Scully", "scully@fbi.gov", true},
		{"Rick Sanchez", "rick@c137.net", true},
		{"Morty Smith", "morty@c137.net", false},
		{"Ned Stark", "ned@winterfell.got", false},
		{"Jon Snow", "jon@winterfell.got", true},
	}

	for _, user := range userData {
		_, err := db.Exec("INSERT INTO users (name, email, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
			user.name, user.email, user.active, time.Now().UTC(), time.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to insert user data: %w", err)
		}
	}

	return nil
}

func truncateTestTables(db *sql.DB) error {
	_, err := db.Exec("TRUNCATE TABLE key_values; TRUNCATE TABLE users;")
	return err
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func requireKVEqual(t *testing.T, expected, actual KeyValue, msgAndArgs ...interface{}) {
	require.Equal(t, expected.ID, actual.ID, msgAndArgs...)
	require.Equal(t, expected.Key, actual.Key, msgAndArgs...)
	require.Equal(t, expected.Value, actual.Value, msgAndArgs...)
}

func requireKVsEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	switch expectedSlice := expected.(type) {
	case []KeyValue:
		switch actualSlice := actual.(type) {
		case []KeyValue:
			require.Len(t, actualSlice, len(expectedSlice), msgAndArgs...)
			for i := range expectedSlice {
				requireKVEqual(t, expectedSlice[i], actualSlice[i], msgAndArgs...)
			}
		case []*KeyValue:
			require.Len(t, actualSlice, len(expectedSlice), msgAndArgs...)
			for i := range expectedSlice {
				require.NotNil(t, actualSlice[i], msgAndArgs...)
				requireKVEqual(t, expectedSlice[i], *actualSlice[i], msgAndArgs...)
			}
		default:
			t.Fatalf("actual must be []KeyValue or []*KeyValue, got %T", actual)
		}
	case []*KeyValue:
		switch actualSlice := actual.(type) {
		case []KeyValue:
			require.Len(t, actualSlice, len(expectedSlice), msgAndArgs...)
			for i := range expectedSlice {
				require.NotNil(t, expectedSlice[i], msgAndArgs...)
				requireKVEqual(t, *expectedSlice[i], actualSlice[i], msgAndArgs...)
			}
		case []*KeyValue:
			require.Len(t, actualSlice, len(expectedSlice), msgAndArgs...)
			for i := range expectedSlice {
				require.NotNil(t, expectedSlice[i], msgAndArgs...)
				require.NotNil(t, actualSlice[i], msgAndArgs...)
				requireKVEqual(t, *expectedSlice[i], *actualSlice[i], msgAndArgs...)
			}
		default:
			t.Fatalf("actual must be []KeyValue or []*KeyValue, got %T", actual)
		}
	default:
		t.Fatalf("expected must be []KeyValue or []*KeyValue, got %T", expected)
	}
}

func TestExists(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("returns true when record exists", func(t *testing.T) {
		exists, err := db.Exists(ctx, &KeyValue{}, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("returns false when record does not exist", func(t *testing.T) {
		exists, err := db.Exists(ctx, &KeyValue{}, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("works with complex WHERE conditions", func(t *testing.T) {
		exists, err := db.Exists(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern AND `value` = $value", Args{
			"pattern": "config.database.%",
			"value":   "localhost",
		})

		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("returns false with complex WHERE conditions that don't match", func(t *testing.T) {
		exists, err := db.Exists(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern AND `value` = $value", Args{
			"pattern": "config.database.%",
			"value":   "nonexistent",
		})

		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("works with value struct instead of pointer", func(t *testing.T) {
		exists, err := db.Exists(ctx, KeyValue{}, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("returns error with invalid struct type", func(t *testing.T) {
		var invalidType []KeyValue
		_, err := db.Exists(ctx, invalidType, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "destination must be a struct or pointer to a struct")
	})

	t.Run("returns error with missing named parameter", func(t *testing.T) {
		_, err := db.Exists(ctx, &KeyValue{}, "WHERE `key` = $nonexistent", Args{
			"key": "config.app.name",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "missing argument for named parameter")
	})
}

func TestCount(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("returns correct count for all records", func(t *testing.T) {
		count, err := db.Count(ctx, &KeyValue{}, "", Args{})

		require.NoError(t, err)
		require.Equal(t, int64(5), count) // There are 5 records in the test data
	})

	t.Run("returns correct count with WHERE condition", func(t *testing.T) {
		count, err := db.Count(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern", Args{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)
		require.Equal(t, int64(2), count) // config.database.host and config.database.port
	})

	t.Run("returns zero count when no records match", func(t *testing.T) {
		count, err := db.Count(ctx, &KeyValue{}, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("works with complex WHERE conditions", func(t *testing.T) {
		count, err := db.Count(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern AND `value` = $value", Args{
			"pattern": "config.database.%",
			"value":   "localhost",
		})

		require.NoError(t, err)
		require.Equal(t, int64(1), count) // Only config.database.host matches
	})

	t.Run("returns zero with complex WHERE conditions that don't match", func(t *testing.T) {
		count, err := db.Count(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern AND `value` = $value", Args{
			"pattern": "config.database.%",
			"value":   "nonexistent",
		})

		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("returns error with invalid struct type", func(t *testing.T) {
		var invalidType []KeyValue
		_, err := db.Count(ctx, invalidType, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "destination must be a struct or pointer to a struct")
	})

	t.Run("returns error with missing named parameter", func(t *testing.T) {
		_, err := db.Count(ctx, &KeyValue{}, "WHERE `key` = $nonexistent", Args{
			"key": "config.app.name",
		})

		require.Error(t, err)
		require.Contains(t, err.Error(), "missing argument for named parameter")
	})
}
