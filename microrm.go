// Package microrm is a lightweight, struct based ORM for Go that simplifies database interactions without completely abstracting SQL.
//
// All methods use named paramters instead of driver specific placeholders in
// SQL queries/fragments. For example, a simple SELECT statement will look like:
//
//	var user User
//	db.Select(&user, "WHERE name = $name AND age > $age", micorm.Args{"name": "Fox", "age": 32})
//
// This enables more readable queries while avoiding the boilerplate+pitfalls of
// positional query arguments.
package microrm

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
)

type (
	// Args is a map of named parameters to their values for SQL queries.
	Args = map[string]any

	// Updates is a map of struct fields to their values for Update* methods
	Updates = map[string]any

	// DB is a wrapper around sql.DB that provides lightweight ORM-like functionality.
	DB struct {
		db             queryable
		modelTypeCache *sync.Map
		time           clock
		// Pluralizer is used to pluralize table names. You can provide your own
		// pluralizer by overriding this field.
		Pluralizer Pluralizer
	}

	// enable using db or tx in the DB struct
	queryable interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}

	// TableNamer is an interface models can implement to override the default
	// `snake_case`d, pluralized table name.
	//
	// *Warning*: This value will be cached, so do not return dynamic values.
	TableNamer interface {
		TableName() string
	}

	// Pluralizer is an interface for pluralizing words
	Pluralizer interface {
		Pluralize(word string) string
	}

	clock interface {
		Now() time.Time
	}
)

// New initializes a new DB instance with the provided sql.DB connection.
func New(db *sql.DB) *DB {
	return &DB{
		db:             db,
		Pluralizer:     defaultPluralizer,
		modelTypeCache: &sync.Map{},
		time:           Time{},
	}
}

// newModelType creates a new modelType for the given destination
func (d *DB) newModelType(model any) (*modelType, error) {
	key := reflect.TypeOf(model)

	if cached, ok := d.modelTypeCache.Load(key); ok {
		return cached.(*modelType), nil
	}

	newModel, err := newModelType(model, d.Pluralizer)

	if err != nil {
		return nil, err
	}

	d.modelTypeCache.Store(key, newModel)

	return newModel, nil
}

// Close closes the underlying database connection.
func (d *DB) Close() error {
	if db, ok := d.db.(*sql.DB); ok {
		return db.Close()
	}
	return nil
}

// Select executes a query and scans the result into the provided model struct or slice of structs.
func (d *DB) Select(ctx context.Context, model any, queryFragment string, args Args) error {
	modelType, err := d.newModelType(model)
	if err != nil {
		return fmt.Errorf("failed to select data: %w", err)
	}

	fragment, queryArgs, err := d.replaceNames(queryFragment, args)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	selectFragment, structFields := d.generateSelect(modelType)
	query := selectFragment + " " + fragment
	rows, err := d.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute Select query: %w", err)
	}
	defer rows.Close()

	if !modelType.isValidSlice && !modelType.isStructPointer {
		return fmt.Errorf("expected a pointer to a slice, or a struct, got %s", reflect.TypeOf(model).String())
	}

	rootType := reflect.TypeOf(model)
	isSlice := rootType.Kind() == reflect.Slice || rootType.Kind() == reflect.Pointer && rootType.Elem().Kind() == reflect.Slice

	if rows.Err() != nil {
		return fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	if isSlice {
		sliceTarget := reflect.ValueOf(model).Elem()

		for rows.Next() {
			row := reflect.New(modelType.elemType).Elem()
			if err := scanStruct(structFields, rows, row); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}

			if modelType.isSliceOfPointers {
				row = row.Addr()
			}

			sliceTarget = reflect.Append(sliceTarget, row)
		}

		reflect.ValueOf(model).Elem().Set(sliceTarget)
	} else {
		row := concreteValue(model)

		// rows.Next() must be called to advance to the first row and check if
		// we actually have results, otherwise return sql.ErrNoRows
		if !rows.Next() {
			return sql.ErrNoRows
		}
		if err := scanStruct(structFields, rows, row); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	return nil
}

// Insert inserts a new record into the database based on the provided struct.
func (d *DB) Insert(ctx context.Context, model any) error {
	modelType, err := d.newModelType(model)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}
	if !modelType.isStructPointer {
		return fmt.Errorf("destination must be a pointer to a struct, got %s", modelType.baseType.Kind())
	}

	var insertColumns strings.Builder
	insertColumnData := make([]any, 0, modelType.numField)
	var insertValuePlaceholders strings.Builder

	value := concreteValue(model)
	now := d.time.Now().UTC()
	touchTimestamp(value, modelType.createdAtFieldIndex, now)
	touchTimestamp(value, modelType.updatedAtFieldIndex, now)

	for _, col := range modelType.columns {
		fieldValue := value.FieldByName(col.Name)

		columnName := col.Tag.Get("db")
		if columnName == "" {
			columnName = snake_case(col.Name)
		}

		if insertColumns.Len() > 0 {
			insertColumns.WriteString(", ")
			insertValuePlaceholders.WriteString(", ")
		}
		insertColumns.WriteString("`" + columnName + "`")
		insertColumnData = append(insertColumnData, fieldValue.Interface())
		insertValuePlaceholders.WriteString("?")
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", modelType.tableName, insertColumns.String(), insertValuePlaceholders.String())

	res, err := d.db.ExecContext(ctx, insertSQL, insertColumnData...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to retrieve last insert ID: %w", err)
	}

	// Attempt to set the ID field if it exists
	idField, ok := d.findIDField(concreteValue(model), modelType)
	if ok && idField.IsValid() && idField.CanSet() {
		switch idField.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			idField.SetInt(id)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if id >= 0 {
				idField.SetUint(uint64(id))
			}
		}
	}

	return nil
}

// Delete deletes records from the database based on the provided struct type
// and SQL fragment with named parameters. The model argument should be a
// pointer to a struct type representing the table to delete from.
//
// It returns the number of rows affected
func (d *DB) Delete(ctx context.Context, modelRef any, queryFragment string, args Args) (int64, error) {
	modelType, err := d.newModelType(modelRef)
	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	fragment, queryArgs, err := d.replaceNames(queryFragment, args)

	if err != nil {
		return 0, fmt.Errorf("failed to prepare delete query: %w", err)
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s %s", modelType.tableName, fragment)
	res, err := d.db.ExecContext(ctx, deleteSQL, queryArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", err)
	}

	return n, nil
}

// DeleteRecords deletes multiple records from the database based on the
// provided slice of structs.  The dest parameter should be a pointer to a slice
// of structs representing the records to delete. It deletes each record by its
// ID inside of a transaction.  If you need to delete in a single statement, use
// `DB.Delete`.
//
// It returns the number of rows affected, or an error if the operation fails.
func (d *DB) DeleteRecords(ctx context.Context, models any) (int64, error) {
	modelType, err := d.newModelType(models)
	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	if !modelType.isValidSlice {
		return 0, fmt.Errorf("destination must be a slice, got %s", modelType.baseType.Kind())
	}

	destValue := concreteValue(models)

	n := int64(0)
	err = d.Transaction(ctx, func(tx *DB) error {
		// For []*T, items are already pointers so we can pass them directly
		if modelType.isSliceOfPointers {
			for i := range destValue.Len() {
				item := destValue.Index(i).Interface()
				nn, err := tx.DeleteRecord(ctx, item)
				if err != nil {
					return err
				}
				n += nn
			}
		} else {
			for i := range destValue.Len() {
				item := destValue.Index(i)
				addressableItem := reflect.New(item.Type())
				addressableItem.Elem().Set(item)

				nn, err := tx.DeleteRecord(ctx, addressableItem.Interface())
				if err != nil {
					return err
				}
				n += nn
			}
		}
		return nil
	})

	if err != nil {
		return 0, err
	}

	return n, nil
}

// DeleteRecord deletes a single record from the database based on the provided struct.
// The dest parameter should be a pointer to a struct representing the record to delete.
//
// It returns the number of rows affected, or an error if the operation fails.
func (d *DB) DeleteRecord(ctx context.Context, model any) (int64, error) {
	modelType, err := d.newModelType(model)

	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	if !modelType.isStructPointer {
		return 0, fmt.Errorf("destination must be a pointer to a struct, got %s", modelType.baseType.Kind())
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id = ?", modelType.tableName)
	idField, ok := d.findIDField(concreteValue(model), modelType)
	if !ok {
		return 0, fmt.Errorf("struct does not have an ID field")
	}

	res, err := d.db.ExecContext(ctx, deleteSQL, idField.Interface())
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", err)
	}

	return n, nil
}

func (d *DB) findIDField(destValue reflect.Value, model *modelType) (reflect.Value, bool) {
	if model.idFieldIndex < 0 {
		return reflect.Value{}, false
	}

	return destValue.Field(model.idFieldIndex), true
}

// Update updates records in the database based on the provided struct type,
// SQL fragment with named parameters, and a map of field-value pairs to update.
// The structType parameter should be a pointer to a struct type representing
// the table to update.
//
// It returns the number of rows affected, or an error if the operation fails.
func (d *DB) Update(ctx context.Context, structType any, queryFragment string, args Args, updates Updates) (int64, error) {
	modelType, err := d.newModelType(structType)
	if err != nil {
		return 0, fmt.Errorf("failed to update data: %w", err)
	}
	if !modelType.isStructPointer && !modelType.isStruct {
		return 0, fmt.Errorf("destination must be a struct or pointer to a struct, got %s", modelType.baseType.Kind())
	}
	if len(updates) == 0 {
		return 0, fmt.Errorf("no updates provided")
	}

	now := d.time.Now().UTC()
	if modelType.updatedAtFieldIndex >= 0 {
		updates = maps.Clone(updates)
		updateField := modelType.elemType.Field(modelType.updatedAtFieldIndex)

		switch updateField.Type.String() {
		case "time.Time":
			updates[updateField.Name] = now
		case "*time.Time":
			updates[updateField.Name] = &now
		case "sql.NullTime":
			updates[updateField.Name] = sql.NullTime{Time: now, Valid: true}
		default:
			return 0, fmt.Errorf("unsupported UpdatedAt field type: %s", updateField.Type.String())
		}
	}

	var setClauses strings.Builder
	updateValues := make([]any, 0, len(updates))

	for _, col := range modelType.columns {
		if _, ok := updates[col.Name]; !ok {
			continue
		}

		name := col.Tag.Get("db")
		if name == "" {
			name = snake_case(col.Name)
		}

		if setClauses.Len() > 0 {
			setClauses.WriteString(", ")
		}
		setClauses.WriteString(fmt.Sprintf("`%s` = ?", name))
		updateValues = append(updateValues, updates[col.Name])
	}

	fragment, whereArgs, err := d.replaceNames(queryFragment, args)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare update query: %w", err)
	}

	updateSQL := fmt.Sprintf("UPDATE %s SET %s %s", modelType.tableName, setClauses.String(), fragment)
	finalArgs := append(updateValues, whereArgs...)

	res, err := d.db.ExecContext(ctx, updateSQL, finalArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute update: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", err)
	}

	return rows, nil
}

// Query calls the underlying sql.DB Query method, but uses named parameters
// like other microrm methods. Query returns sql.Rows, which the caller is
// responsible for closing.
func (d *DB) Query(ctx context.Context, sql string, args map[string]any) (*sql.Rows, error) {
	sql, argSlice, err := d.replaceNames(sql, args)
	if err != nil {
		return nil, err
	}
	return d.db.QueryContext(ctx, sql, argSlice...)
}

// Exec calls the underlying sql.DB Exec method, but uses named parameters like
// other microrm methods.
func (d *DB) Exec(ctx context.Context, sql string, args map[string]any) (sql.Result, error) {
	sql, argSlice, err := d.replaceNames(sql, args)
	if err != nil {
		return nil, err
	}
	return d.db.ExecContext(ctx, sql, argSlice...)
}

// UpdateRecord updates a single record in the database based on the provided struct.
// The dest parameter should be a pointer to a struct of the record to update.
func (d *DB) UpdateRecord(ctx context.Context, model any, updates Updates) error {
	modelType, err := d.newModelType(model)
	if err != nil {
		return fmt.Errorf("failed to update data: %w", err)
	}
	if !modelType.isStructPointer {
		return fmt.Errorf("destination must be a pointer to a struct, got %s", modelType.baseType.Kind())
	}
	if len(updates) == 0 {
		return fmt.Errorf("no updates provided")
	}

	value := concreteValue(model)
	idField, ok := d.findIDField(value, modelType)
	if !ok {
		return fmt.Errorf("struct does not have an ID field")
	}

	now := d.time.Now().UTC()
	if modelType.updatedAtFieldIndex >= 0 {
		updates = maps.Clone(updates)
		updateField := modelType.elemType.Field(modelType.updatedAtFieldIndex)

		switch updateField.Type.String() {
		case "time.Time":
			updates[updateField.Name] = now
		case "*time.Time":
			updates[updateField.Name] = &now
		case "sql.NullTime":
			updates[updateField.Name] = sql.NullTime{Time: now, Valid: true}
		default:
			return fmt.Errorf("unsupported UpdatedAt field type: %s", updateField.Type.String())
		}
	}

	var setClauses strings.Builder
	updateValues := make([]any, 0, len(updates))

	for fieldName, val := range updates {
		field, ok := modelType.elemType.FieldByName(fieldName)
		if !ok || !field.IsExported() {
			return fmt.Errorf("cannot update missing or unexported field: %s", fieldName)
		}
		col := field.Tag.Get("db")
		if col == "" {
			col = snake_case(field.Name)
		}
		if setClauses.Len() > 0 {
			setClauses.WriteString(", ")
		}
		setClauses.WriteString(fmt.Sprintf("`%s` = ?", col))
		updateValues = append(updateValues, val)
	}

	updateSQL := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", modelType.tableName, setClauses.String())
	updateValues = append(updateValues, idField.Interface())
	_, err = d.db.ExecContext(ctx, updateSQL, updateValues...)
	if err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	for fieldName, val := range updates {
		field := value.FieldByName(fieldName)
		if field.IsValid() && field.CanSet() {
			field.Set(reflect.ValueOf(val))
		}
	}

	return nil
}

// Transaction executes the provided function within a database transaction. If
// the function returns an error, the transaction is rolled back, otherwise it
// is committed.
//
// Transactions can not be nested at this time.
func (d *DB) Transaction(ctx context.Context, fn func(tx *DB) error) error {
	if _, ok := d.db.(*sql.DB); !ok {
		return fmt.Errorf("nested transactions are not supported")
	}
	tx, err := d.db.(*sql.DB).BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	txDB := &DB{
		db:             tx,
		modelTypeCache: d.modelTypeCache,
		Pluralizer:     d.Pluralizer,
		time:           d.time,
	}

	err = fn(txDB)
	return err
}

func concreteValue(dest any) reflect.Value {
	v := reflect.ValueOf(dest)
	if v.Kind() == reflect.Pointer {
		return v.Elem()
	}
	return v
}

func scanStruct(fields []string, rows *sql.Rows, dest reflect.Value) error {
	scanArgs := make([]any, 0, len(fields))

	for _, fieldName := range fields {
		field := dest.FieldByName(fieldName)
		scanArgs = append(scanArgs, field.Addr().Interface())
	}

	err := rows.Scan(scanArgs...)
	if err != nil {
		return fmt.Errorf("failed to scan row into struct: %w", err)
	}

	return nil
}

func (d *DB) replaceNames(rawSql string, args Args) (string, []any, error) {
	finalArgs := make([]any, 0, len(args))
	builder := strings.Builder{}

	sql := []rune(rawSql)
	for i := 0; i < len(sql); i++ {
		b := sql[i]

		// Double $$ is an escaped $, following sql ' and '' semantics
		if b == '$' && i+1 < len(sql) && sql[i+1] == '$' {
			builder.WriteRune('$')
			i++ // skip the second $
			continue
		}

		if b != '$' {
			builder.WriteRune(b)
			continue
		}

		var name strings.Builder
		if i+1 < len(sql) && (unicode.IsLetter(sql[i+1]) || sql[i+1] == '_') {
			for j := i + 1; j < len(sql); j++ {
				if unicode.IsLetter(sql[j]) || unicode.IsDigit(sql[j]) || sql[j] == '_' {
					name.WriteRune(sql[j])
				} else {
					break
				}
			}

			if name.Len() > 0 {
				// catch the outer loop up to the end of the name
				i += name.Len()
				if _, ok := args[name.String()]; !ok {
					return "", nil, fmt.Errorf("missing argument for named parameter: %s", name.String())
				}
				finalArgs = append(finalArgs, args[name.String()])
				builder.WriteRune('?')
			} else {
				builder.WriteRune('$')
			}
		} else {
			builder.WriteRune('$')
		}
	}

	return builder.String(), finalArgs, nil
}

// generateSelect creates a SELECT SQL statement based on the struct type, mapping struct fields to database columns.
// it returns the SQL string and a slice of column names to be used in scanning.
func (d *DB) generateSelect(model *modelType) (string, []string) {
	columns := make([]string, 0, model.numField)
	var columnStr strings.Builder

	for _, col := range model.columns {
		columnName := col.Tag.Get("db")
		if columnName == "" {
			columnName = snake_case(col.Name)
		}
		columns = append(columns, col.Name)
		if len(columns) > 1 {
			columnStr.WriteString(", ")
		}
		columnStr.WriteString("`" + model.tableName + "`.")
		columnStr.WriteString("`" + columnName + "`")
	}

	return fmt.Sprintf("SELECT %s FROM %s", columnStr.String(), model.tableName), columns
}

func snake_case(name string) string {
	snaked := strings.Builder{}

	for i, r := range name {
		if unicode.IsUpper(r) {
			if i > 0 {
				snaked.WriteRune('_')
			}
			snaked.WriteRune(unicode.ToLower(r))
		} else {
			snaked.WriteRune(r)
		}
	}

	return snaked.String()
}

func touchTimestamp(value reflect.Value, fieldIndex int, now time.Time) {
	if fieldIndex < 0 {
		return
	}

	timestamp := value.Field(fieldIndex)

	switch timestamp.Type().String() {
	case "time.Time":
		timestamp.Set(reflect.ValueOf(now))
	case "*time.Time":
		timestamp.Set(reflect.ValueOf(&now))
	case "sql.NullTime":
		timestamp.Set(reflect.ValueOf(sql.NullTime{Time: now, Valid: true}))
	}
}
