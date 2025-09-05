package microrm

import (
	"context"
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
	ctx := context.Background()

	t.Run("select single key-value", func(t *testing.T) {
		var kv KeyValue
		err := testDB.Select(ctx, &kv, "WHERE `key` = $key", map[string]any{
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
		err := testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)

		expectedKVs := []KeyValue{
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
		}
		require.Equal(t, expectedKVs, kvs)
	})

	t.Run("select multiple key-values into slice of pointers", func(t *testing.T) {
		var kvs []*KeyValue
		err := testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)
		require.Len(t, kvs, 2)

		expectedKVs := []*KeyValue{
			{ID: 1, Key: "config.database.host", Value: "localhost"},
			{ID: 2, Key: "config.database.port", Value: "3306"},
		}

		for i, kv := range kvs {
			require.NotNil(t, kv)
			require.Equal(t, *expectedKVs[i], *kv)
		}
	})

	t.Run("select all key-values", func(t *testing.T) {
		var kvs []KeyValue
		err := testDB.Select(ctx, &kvs, "ORDER BY `key`", map[string]any{})

		require.NoError(t, err)

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
		err := testDB.Select(ctx, &kv, "WHERE `key` = $key", map[string]any{
			"key": "non.existent.key",
		})

		require.Error(t, err, sql.ErrNoRows)
		require.Equal(t, KeyValue{}, kv)
	})
}

func TestInsert(t *testing.T) {
	ctx := context.Background()

	t.Run("populates ID of inserted structs", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.insert.key",
			Value: "test insert value",
		}

		require.Equal(t, 0, kv.ID)

		err := testDB.Insert(ctx, kv)
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

		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)

		require.Equal(t, 999, kv.ID, "Pre-existing ID should be preserved")
	})

	t.Run("can insert data", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.database.verification",
			Value: "thetruthisoutthere",
		}

		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)

		var retrievedKV KeyValue
		row := testDB.db.QueryRowContext(ctx, "SELECT id, `key`, value FROM key_values WHERE id = ?", kv.ID)
		err = row.Scan(&retrievedKV.ID, &retrievedKV.Key, &retrievedKV.Value)
		require.NoError(t, err)

		require.Equal(t, kv.ID, retrievedKV.ID)
		require.Equal(t, kv.Key, retrievedKV.Key)
		require.Equal(t, kv.Value, retrievedKV.Value)
	})
}

func TestTransaction(t *testing.T) {
	ctx := context.Background()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("successful transaction commits changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := testDB.Transaction(ctx, func(tx *DB) error {
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
		err = testDB.Select(ctx, &retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.commit",
		})
		require.NoError(t, err)
		require.Equal(t, insertedKV.ID, retrievedKV.ID)
		require.Equal(t, "test.transaction.commit", retrievedKV.Key)
		require.Equal(t, "transaction commit test", retrievedKV.Value)
	})

	t.Run("failed transaction rolls back changes", func(t *testing.T) {
		var insertedKV *KeyValue

		err := testDB.Transaction(ctx, func(tx *DB) error {
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
		err = testDB.Select(ctx, &retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.rollback",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("multiple operations in transaction", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := testDB.Transaction(ctx, func(tx *DB) error {
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
			err = tx.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
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
		err = testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "test.transaction.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 2)
		require.Equal(t, "test.transaction.multi.1", kvs[0].Key)
		require.Equal(t, "test.transaction.multi.2", kvs[1].Key)
	})

	t.Run("transaction rollback with multiple operations", func(t *testing.T) {
		var kv1, kv2 *KeyValue

		err := testDB.Transaction(ctx, func(tx *DB) error {
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
		err = testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern", map[string]any{
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

			_ = testDB.Transaction(ctx, func(tx *DB) error {
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
		err := testDB.Select(ctx, &retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.transaction.panic",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("nested transactions not supported", func(t *testing.T) {
		err := testDB.Transaction(ctx, func(tx *DB) error {
			return tx.Transaction(ctx, func(nestedTx *DB) error {
				return nil
			})
		})

		require.Error(t, err)
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("delete single record by key", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.delete.single",
			Value: "to be deleted",
		}
		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)

		var retrievedKV KeyValue
		err = testDB.Select(ctx, &retrievedKV, "WHERE `key` = $key", map[string]any{
			"key": "test.delete.single",
		})
		require.NoError(t, err)

		rowsAffected, err := testDB.Delete(ctx, &KeyValue{}, "WHERE `key` = $key", map[string]any{
			"key": "test.delete.single",
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = testDB.Select(ctx, &deletedKV, "WHERE `key` = $key", map[string]any{
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
			err := testDB.Insert(ctx, &testRecords[i])
			require.NoError(t, err)
		}

		var kvs []KeyValue
		err := testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "test.delete.multi.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 3)

		rowsAffected, err := testDB.Delete(ctx, &KeyValue{}, "WHERE `key` LIKE $pattern", map[string]any{
			"pattern": "test.delete.multi.%",
		})
		require.NoError(t, err)
		require.Equal(t, int64(3), rowsAffected)

		var deletedKVs []KeyValue
		err = testDB.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", map[string]any{
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
		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)
		insertedID := kv.ID

		rowsAffected, err := testDB.Delete(ctx, &KeyValue{}, "WHERE id = $id", map[string]any{
			"id": insertedID,
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = testDB.Select(ctx, &deletedKV, "WHERE id = $id", map[string]any{
			"id": insertedID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("delete non-existent record returns zero rows affected", func(t *testing.T) {
		rowsAffected, err := testDB.Delete(ctx, &KeyValue{}, "WHERE `key` = $key", map[string]any{
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
			err := testDB.Insert(ctx, &testRecords[i])
			require.NoError(t, err)
		}

		rowsAffected, err := testDB.Delete(ctx, &KeyValue{}, "WHERE `key` LIKE $keyPattern AND `value` LIKE $valuePattern", map[string]any{
			"keyPattern":   "test.delete.complex.remove%",
			"valuePattern": "%remove%",
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var remainingKVs []KeyValue
		err = testDB.Select(ctx, &remainingKVs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
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

	t.Run("delete record by ID field", func(t *testing.T) {
		kv := &KeyValue{
			Key:   "test.deleterecord.basic",
			Value: "basic delete test",
		}
		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)
		insertedID := kv.ID

		var retrievedKV KeyValue
		err = testDB.Select(ctx, &retrievedKV, "WHERE id = $id", map[string]any{
			"id": insertedID,
		})
		require.NoError(t, err)
		require.Equal(t, insertedID, retrievedKV.ID)

		rowsAffected, err := testDB.DeleteRecord(ctx, kv)
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = testDB.Select(ctx, &deletedKV, "WHERE id = $id", map[string]any{
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

		rowsAffected, err := testDB.DeleteRecord(ctx, noID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct does not have an ID field")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete record with custom db tag for ID", func(t *testing.T) {
		type CustomIDStruct struct {
			CustomID int    `db:"id"`
			Key      string `db:"key"`
			Value    string `db:"value"`
		}

		testDB.MapNameToTable("CustomIDStruct", "key_values")

		kv := &KeyValue{
			Key:   "test.deleterecord.customid",
			Value: "custom ID test",
		}
		err := testDB.Insert(ctx, kv)
		require.NoError(t, err)
		require.NotEqual(t, 0, kv.ID)

		customKV := &CustomIDStruct{
			CustomID: kv.ID,
			Key:      "test.deleterecord.customid",
			Value:    "custom ID test",
		}

		rowsAffected, err := testDB.DeleteRecord(ctx, customKV)
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		var deletedKV KeyValue
		err = testDB.Select(ctx, &deletedKV, "WHERE id = $id", map[string]any{
			"id": kv.ID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})
}

func TestDeleteRecords(t *testing.T) {
	ctx := context.Background()

	t.Run("delete multiple records by slice", func(t *testing.T) {
		testRecords := []*KeyValue{
			{Key: "test.deleterecords.1", Value: "first record"},
			{Key: "test.deleterecords.2", Value: "second record"},
			{Key: "test.deleterecords.3", Value: "third record"},
		}

		for _, kv := range testRecords {
			err := testDB.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		var kvs []KeyValue
		err := testDB.Select(ctx, &kvs, "WHERE `key` LIKE $pattern ORDER BY `key`", map[string]any{
			"pattern": "test.deleterecords.%",
		})
		require.NoError(t, err)
		require.Len(t, kvs, 3)

		rowsAffected, err := testDB.DeleteRecords(ctx, testRecords)
		require.NoError(t, err)
		require.Equal(t, int64(3), rowsAffected)

		var deletedKVs []KeyValue
		err = testDB.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", map[string]any{
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
			err := testDB.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		rowsAffected, err := testDB.DeleteRecords(ctx, &testRecords)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var deletedKVs []KeyValue
		err = testDB.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", map[string]any{
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
			err := testDB.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		testRecordValues := []KeyValue{
			*testRecordPtrs[0],
			*testRecordPtrs[1],
		}

		rowsAffected, err := testDB.DeleteRecords(ctx, testRecordValues)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		var deletedKVs []KeyValue
		err = testDB.Select(ctx, &deletedKVs, "WHERE `key` LIKE $pattern", map[string]any{
			"pattern": "test.deleterecords.values.%",
		})
		require.NoError(t, err)
		require.Len(t, deletedKVs, 0)
	})

	t.Run("delete empty slice returns zero rows affected", func(t *testing.T) {
		emptyRecords := []*KeyValue{}

		rowsAffected, err := testDB.DeleteRecords(ctx, emptyRecords)
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

		rowsAffected, err := testDB.DeleteRecords(ctx, noIDRecords)
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct does not have an ID field")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete records with mixed success and failure should rollback", func(t *testing.T) {
		validKV := &KeyValue{
			Key:   "test.deleterecords.rollback.valid",
			Value: "valid record",
		}
		err := testDB.Insert(ctx, validKV)
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

		rowsAffected, err := testDB.DeleteRecords(ctx, mixedRecords)
		require.Error(t, err)
		require.Equal(t, int64(0), rowsAffected)

		var retrievedKV KeyValue
		err = testDB.Select(ctx, &retrievedKV, "WHERE id = $id", map[string]any{
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

		rowsAffected, err := testDB.DeleteRecords(ctx, singleRecord)
		require.Error(t, err)
		require.Contains(t, err.Error(), "destination must be a slice")
		require.Equal(t, int64(0), rowsAffected)
	})

	t.Run("delete records with custom db tag for ID", func(t *testing.T) {
		type CustomIDStruct struct {
			CustomID int    `db:"id"`
			Key      string `db:"key"`
			Value    string `db:"value"`
		}

		testDB.MapNameToTable("CustomIDStruct", "key_values")

		testKVs := []*KeyValue{
			{Key: "test.deleterecords.customid.1", Value: "custom ID test 1"},
			{Key: "test.deleterecords.customid.2", Value: "custom ID test 2"},
		}

		for _, kv := range testKVs {
			err := testDB.Insert(ctx, kv)
			require.NoError(t, err)
			require.NotEqual(t, 0, kv.ID)
		}

		customRecords := []*CustomIDStruct{
			{CustomID: testKVs[0].ID, Key: "test.deleterecords.customid.1", Value: "custom ID test 1"},
			{CustomID: testKVs[1].ID, Key: "test.deleterecords.customid.2", Value: "custom ID test 2"},
		}

		rowsAffected, err := testDB.DeleteRecords(ctx, customRecords)
		require.NoError(t, err)
		require.Equal(t, int64(2), rowsAffected)

		for _, kv := range testKVs {
			var deletedKV KeyValue
			err = testDB.Select(ctx, &deletedKV, "WHERE id = $id", map[string]any{
				"id": kv.ID,
			})
			require.Error(t, err)
			require.Equal(t, sql.ErrNoRows, err)
		}
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
