package microrm

import (
	"context"
	"fmt"
	"reflect"
)

// ModelDB provides a type-safe, generic interface for database operations on a specific model type T.
// It wraps the underlying DB instance and provides methods that automatically handle the model type.
type ModelDB[T any] struct {
	db *DB
}

// M returns a ModelDB[T] for the given type T, providing an easy-to-use API to
// run queries against that model/table.
func M[T any](db *DB) ModelDB[T] {
	var t T
	if reflect.TypeOf(t).Kind() != reflect.Struct {
		panic(fmt.Sprintf("ModelDB can only be created for struct types, got %s", reflect.TypeOf(t)))
	}

	return ModelDB[T]{
		db: db,
	}
}

// Many executes a SELECT query and returns multiple records of type T.
// The queryFragment should contain the WHERE clause and any other SQL after SELECT.
func (m *ModelDB[T]) Many(ctx context.Context, queryFragment string, args Args) ([]T, error) {
	var records []T
	err := m.db.Select(ctx, &records, queryFragment, args)
	if err != nil {
		return nil, err
	}

	return records, nil
}

// Find executes a SELECT query and returns a single record of type T.
// Returns an error if no record is found or if multiple records match.
func (m *ModelDB[T]) Find(ctx context.Context, queryFragment string, args Args) (T, error) {
	var record T

	err := m.db.Select(ctx, &record, queryFragment, args)
	if err != nil {
		return record, err
	}

	return record, nil
}

// Insert inserts a new record of type T into the database.
// The ID field will be populated with the auto-generated primary key if applicable.
func (m *ModelDB[T]) Insert(ctx context.Context, model *T) error {
	return m.db.Insert(ctx, model)
}

// Update executes an UPDATE query for records of type T matching the query fragment.
// Returns the number of rows affected.
func (m *ModelDB[T]) Update(ctx context.Context, queryFragment string, args Args, updates Updates) (int64, error) {
	var t T
	return m.db.Update(ctx, t, queryFragment, args, updates)
}

// Delete executes a DELETE query for records of type T matching the query fragment.
// Returns the number of rows affected.
func (m *ModelDB[T]) Delete(ctx context.Context, queryFragment string, args Args) (int64, error) {
	var t T
	return m.db.Delete(ctx, t, queryFragment, args)
}

// UpdateRecord updates a specific record by its ID field.
// The model must have an ID field that will be used to identify the record to update.
func (m *ModelDB[T]) UpdateRecord(ctx context.Context, model *T, updates Updates) error {
	return m.db.UpdateRecord(ctx, model, updates)
}

// DeleteRecord deletes a specific record by its ID field.
// The model must have an ID field that will be used to identify the record to delete.
// Returns an error if the model doesn't have an ID field.
func (m *ModelDB[T]) DeleteRecord(ctx context.Context, model *T) (int64, error) {
	return m.db.DeleteRecord(ctx, model)
}
