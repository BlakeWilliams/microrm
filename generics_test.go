package dbmap

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelDB_Many(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("returns multiple key-values", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		kvs, err := kvDB.Many(ctx, "WHERE `key` LIKE $pattern ORDER BY `key`", Args{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)
		require.Len(t, kvs, 2)
		require.Equal(t, "config.database.host", kvs[0].Key)
		require.Equal(t, "localhost", kvs[0].Value)
		require.Equal(t, "config.database.port", kvs[1].Key)
		require.Equal(t, "3306", kvs[1].Value)
	})

	t.Run("returns empty slice when no matches", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		kvs, err := kvDB.Many(ctx, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.NoError(t, err)
		require.Len(t, kvs, 0)
	})
}

func TestModelDB_Find(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("finds single key-value", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		kv, err := kvDB.Find(ctx, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.NoError(t, err)
		require.Equal(t, "config.app.name", kv.Key)
		require.Equal(t, "MicroORM", kv.Value)
	})

	t.Run("returns error when no record found", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		_, err := kvDB.Find(ctx, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})
}

func TestModelDB_Insert(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("inserts value type", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		kv := KeyValue{
			Key:   "test.generics.insert.value",
			Value: "test value insert",
		}

		err := kvDB.Insert(ctx, &kv)
		require.NoError(t, err)
		require.NotZero(t, kv.ID)

		var retrieved KeyValue
		err = db.Select(ctx, &retrieved, "WHERE `key` = $key", Args{
			"key": "test.generics.insert.value",
		})
		require.NoError(t, err)
		require.Equal(t, kv.ID, retrieved.ID)
		require.Equal(t, "test value insert", retrieved.Value)
	})
}

func TestModelDB_Update(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("updates with value type", func(t *testing.T) {
		orig := &KeyValue{Key: "test.generics.update.value", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))

		kvDB := M[KeyValue](db)
		rows, err := kvDB.Update(ctx, "WHERE `key` = $key", Args{
			"key": "test.generics.update.value",
		}, Updates{
			"Value": "after",
		})

		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		var updated KeyValue
		err = db.Select(ctx, &updated, "WHERE `key` = $key", Args{
			"key": "test.generics.update.value",
		})
		require.NoError(t, err)
		require.Equal(t, "after", updated.Value)
	})

	t.Run("returns zero rows when no match", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		rows, err := kvDB.Update(ctx, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		}, Updates{
			"Value": "whatever",
		})

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})
}

func TestModelDB_Delete(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("deletes rows", func(t *testing.T) {
		orig := &KeyValue{Key: "test.generics.delete.value", Value: "to be deleted"}
		require.NoError(t, db.Insert(ctx, orig))

		kvDB := M[KeyValue](db)
		rows, err := kvDB.Delete(ctx, "WHERE `key` = $key", Args{
			"key": "test.generics.delete.value",
		})

		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		var deleted KeyValue
		err = db.Select(ctx, &deleted, "WHERE `key` = $key", Args{
			"key": "test.generics.delete.value",
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("returns zero rows when no match", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		rows, err := kvDB.Delete(ctx, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.NoError(t, err)
		require.Equal(t, int64(0), rows)
	})
}

func TestModelDB_UpdateRecord(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("updates record", func(t *testing.T) {
		orig := &KeyValue{Key: "test.generics.updaterecord.value", Value: "before"}
		require.NoError(t, db.Insert(ctx, orig))
		originalID := orig.ID

		kvDB := M[KeyValue](db)
		err := kvDB.UpdateRecord(ctx, orig, Updates{
			"Value": "after",
		})

		require.NoError(t, err)
		require.Equal(t, originalID, orig.ID)

		var updated KeyValue
		err = db.Select(ctx, &updated, "WHERE id = $id", Args{
			"id": originalID,
		})
		require.NoError(t, err)
		require.Equal(t, "after", updated.Value)
	})
}

func TestModelDB_DeleteRecord(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("deletes record", func(t *testing.T) {
		orig := &KeyValue{Key: "test.generics.deleterecord.value", Value: "to be deleted"}
		require.NoError(t, db.Insert(ctx, orig))
		originalID := orig.ID

		kvDB := M[KeyValue](db)
		n, err := kvDB.DeleteRecord(ctx, orig)
		require.Equal(t, int64(1), n)
		require.NoError(t, err)

		var deleted KeyValue
		err = db.Select(ctx, &deleted, "WHERE id = $id", Args{
			"id": originalID,
		})
		require.Error(t, err)
		require.Equal(t, sql.ErrNoRows, err)
	})
}

func TestModelDB_Exists(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("returns true when record exists", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		exists, err := kvDB.Exists(ctx, "WHERE `key` = $key", Args{
			"key": "config.app.name",
		})

		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestModelDB_Count(t *testing.T) {
	ctx := context.Background()
	sqlDB := setupDB(t)
	db := New(sqlDB)

	t.Run("returns correct count with WHERE condition", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		count, err := kvDB.Count(ctx, "WHERE `key` LIKE $pattern", Args{
			"pattern": "config.database.%",
		})

		require.NoError(t, err)
		require.Equal(t, int64(2), count) // config.database.host and config.database.port
	})

	t.Run("returns zero count when no records match", func(t *testing.T) {
		kvDB := M[KeyValue](db)
		count, err := kvDB.Count(ctx, "WHERE `key` = $key", Args{
			"key": "nonexistent.key",
		})

		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})
}
