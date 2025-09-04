// Package microrm is a lightweight, struct based ORM for Go that simplifies database interactions without completely abstracting SQL.
package microrm

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

var ErrArrayNotSupported = errors.New("array types are not supported")

// enable using db or tx in the DB struct
type queryable interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
}

// DB represents a database connection and provides methods for executing queries and mapping results to structs.
type DB struct {
	db      queryable
	nameMap map[string]string
	mu      sync.Mutex
}

// New initializes a new DB instance with the provided sql.DB connection.
func New(db *sql.DB) *DB {
	return &DB{db: db, nameMap: make(map[string]string)}
}

func (d *DB) MapNameToTable(structName, tableName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nameMap[structName] = tableName
}

// Close closes the underlying database connection.
func (d *DB) Close() error {
	if db, ok := d.db.(*sql.DB); ok {
		return db.Close()
	}
	return nil
}

// Select executes a query and scans the result into the provided destination struct or slice of structs.
func (d *DB) Select(dest any, rawSql string, rawArgs map[string]any) error {
	destType, err := identifyRootType(dest)
	if err != nil {
		return fmt.Errorf("failed to select data: %w", err)
	}

	fragment, args, err := d.replaceNames(rawSql, rawArgs)
	if err != nil {
		return fmt.Errorf("failed to prepare query: %w", err)
	}
	selectFragment, structFields := d.generateSelect(destType)
	query := selectFragment + " " + fragment
	rows, err := d.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute Select query: %w", err)
	}
	defer rows.Close()

	rootType := reflect.TypeOf(dest)
	isSlice := rootType.Kind() == reflect.Slice || rootType.Kind() == reflect.Pointer && rootType.Elem().Kind() == reflect.Slice

	if rows.Err() != nil {
		return fmt.Errorf("error occurred during row iteration: %w", rows.Err())
	}

	if isSlice {
		sliceTarget := reflect.ValueOf(dest).Elem()

		for rows.Next() {
			row := reflect.New(destType).Elem()
			if err := scanStruct(structFields, rows, row); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			sliceTarget = reflect.Append(sliceTarget, row)
		}

		reflect.ValueOf(dest).Elem().Set(sliceTarget)
	} else {
		row := reflect.ValueOf(dest)
		if row.Kind() == reflect.Ptr {
			row = row.Elem()
		}

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
func (d *DB) Insert(dest any) error {
	destType := reflect.TypeOf(dest)
	if destType.Kind() != reflect.Pointer {
		return fmt.Errorf("destination must be a pointer to a struct, got %s", destType.Kind())
	}

	destRootType := destType.Elem()

	if destRootType.Kind() != reflect.Struct {
		return fmt.Errorf("destination must be a struct, got %s", destType.Kind())
	}

	var insertColumns strings.Builder
	insertColumnData := make([]any, 0, destRootType.NumField())
	var insertValuePlaceholders strings.Builder

	for i := 0; i < destRootType.NumField(); i++ {
		field := destRootType.Field(i)
		if !field.IsExported() {
			continue
		}

		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = snake_case(field.Name)
		}

		if insertColumns.Len() > 0 {
			insertColumns.WriteString(", ")
			insertValuePlaceholders.WriteString(", ")
		}
		insertColumns.WriteString("`" + columnName + "`")
		insertColumnData = append(insertColumnData, reflect.ValueOf(dest).Elem().FieldByName(field.Name).Interface())
		insertValuePlaceholders.WriteString("?")
	}

	tableName := destRootType.Name()
	if rename, ok := d.nameMap[tableName]; ok {
		tableName = rename
	} else {
		tableName = snake_case(destType.Name())
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, insertColumns.String(), insertValuePlaceholders.String())

	res, err := d.db.Exec(insertSQL, insertColumnData...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("failed to retrieve last insert ID: %w", err)
	}

	// Attempt to set the ID field if it exists
	idField := reflect.ValueOf(dest).Elem().FieldByName("ID")
	if idField.IsValid() && idField.CanSet() && (idField.Kind() == reflect.Int || idField.Kind() == reflect.Int64) {
		idField.SetInt(id)
	}

	return nil
}

// Delete deletes records from the database based on the provided struct type
// and SQL fragment with named parameters.	 The dest parameter should be a
// pointer to a struct type representing the table to delete from.
//
// It returns the number of rows affected, or an error if the operation fails.
func (d *DB) Delete(dest any, rawSql string, rawArgs map[string]any) (int64, error) {
	destType, err := identifyRootType(dest)
	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	fragment, args, err := d.replaceNames(rawSql, rawArgs)

	if err != nil {
		return 0, fmt.Errorf("failed to prepare delete query: %w", err)
	}

	tableName := snake_case(destType.Name())
	if rename, ok := d.nameMap[destType.Name()]; ok {
		tableName = rename
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s %s", tableName, fragment)
	res, err := d.db.Exec(deleteSQL, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", err)
	}

	return n, nil
}

func (d *DB) DeleteRecord(dest any) (int64, error) {
	destType, err := identifyRootType(dest)
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() == reflect.Pointer {
		destValue = destValue.Elem()
	}

	if err != nil {
		return 0, fmt.Errorf("failed to delete data: %w", err)
	}

	if destType.Kind() == reflect.Pointer && destType.Elem().Kind() != reflect.Struct {
		return 0, fmt.Errorf("destination must be a struct, got pointer to %s", destType.Elem().Kind())
	} else if destType.Kind() != reflect.Struct {
		return 0, fmt.Errorf("destination must be a struct, got %s", destType.Kind())
	}

	tableName := snake_case(destType.Name())
	if rename, ok := d.nameMap[destType.Name()]; ok {
		tableName = rename
	}

	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName)
	idField, ok := d.findIDField(destValue)
	if !ok {
		return 0, fmt.Errorf("struct does not have an ID field")
	}

	res, err := d.db.Exec(deleteSQL, idField.Interface())
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", err)
	}

	return n, nil
}

func (d *DB) findIDField(destValue reflect.Value) (reflect.Value, bool) {
	destValue.Type()
	for i := 0; i < destValue.NumField(); i++ {
		field := destValue.Type().Field(i)
		if field.Name == "ID" || field.Tag.Get("db") == "id" {
			return destValue.Field(i), true
		}
	}
	return reflect.Value{}, false
}

// Transaction executes the provided function within a database transaction. If
// the function returns an error, the transaction is rolled back, otherwise it
// is committed.
//
// Transactions can not be nested at this time.
func (d *DB) Transaction(fn func(tx *DB) error) error {
	if _, ok := d.db.(*sql.DB); !ok {
		return fmt.Errorf("nested transactions are not supported")
	}
	tx, err := d.db.(*sql.DB).Begin()
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
		db:      tx,
		nameMap: d.nameMap,
	}

	err = fn(txDB)
	return err
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

func (d *DB) replaceNames(rawSql string, args map[string]any) (string, []any, error) {
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
func (d *DB) generateSelect(destType reflect.Type) (string, []string) {
	tableName := destType.Name()
	if rename, ok := d.nameMap[tableName]; ok {
		tableName = rename
	} else {
		tableName = snake_case(destType.Name())
	}

	columns := make([]string, 0, destType.NumField())
	var columnStr strings.Builder
	for i := 0; i < destType.NumField(); i++ {
		field := destType.Field(i)
		if !field.IsExported() {
			continue
		}

		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = snake_case(field.Name)
		}
		columns = append(columns, field.Name)
		if i > 0 {
			columnStr.WriteString(", ")
		}
		columnStr.WriteString("`" + columnName + "`")
	}

	return fmt.Sprintf("SELECT %s FROM %s", columnStr.String(), tableName), columns
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

func identifyRootType(dest any) (reflect.Type, error) {
	current := reflect.TypeOf(dest)

	for {
		if current.Kind() == reflect.Ptr || current.Kind() == reflect.Slice {
			current = current.Elem()
			continue
		} else if current.Kind() == reflect.Array {
			return nil, ErrArrayNotSupported
		}
		break
	}

	if current.Kind() != reflect.Struct {
		return nil, fmt.Errorf("destination must be a struct or slice of structs, got %s", current.Kind())
	}

	return current, nil
}
