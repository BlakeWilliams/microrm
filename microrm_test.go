package dbmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_replaceNames(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name         string
		rawSql       string
		args         map[string]any
		expectedSql  string
		expectedArgs []any
		shouldError  bool
		errorMsg     string
	}{
		{
			name:         "simple named parameter",
			rawSql:       "SELECT * FROM users WHERE id = $id",
			args:         map[string]any{"id": 123},
			expectedSql:  "SELECT * FROM users WHERE id = ?",
			expectedArgs: []any{123},
		},
		{
			name:         "multiple named parameters",
			rawSql:       "SELECT * FROM users WHERE id = $id AND name = $name",
			args:         map[string]any{"id": 123, "name": "John"},
			expectedSql:  "SELECT * FROM users WHERE id = ? AND name = ?",
			expectedArgs: []any{123, "John"},
		},
		{
			name:         "same parameter used multiple times",
			rawSql:       "SELECT * FROM logs WHERE user_id = $user_id OR admin_id = $user_id",
			args:         map[string]any{"user_id": 456},
			expectedSql:  "SELECT * FROM logs WHERE user_id = ? OR admin_id = ?",
			expectedArgs: []any{456, 456},
		},
		{
			name:         "parameter with underscore",
			rawSql:       "SELECT * FROM users WHERE user_id = $user_id",
			args:         map[string]any{"user_id": 789},
			expectedSql:  "SELECT * FROM users WHERE user_id = ?",
			expectedArgs: []any{789},
		},
		{
			name:         "parameter with numbers",
			rawSql:       "SELECT * FROM table1 WHERE field2 = $param123",
			args:         map[string]any{"param123": "value"},
			expectedSql:  "SELECT * FROM table1 WHERE field2 = ?",
			expectedArgs: []any{"value"},
		},
		{
			name:         "escaped $ symbol",
			rawSql:       "SELECT * FROM users WHERE price LIKE '$$19.99'",
			args:         map[string]any{},
			expectedSql:  "SELECT * FROM users WHERE price LIKE '$19.99'",
			expectedArgs: []any{},
		},
		{
			name:         "$ symbol not followed by valid identifier",
			rawSql:       "SELECT * FROM users WHERE price = '$' OR id = $id",
			args:         map[string]any{"id": 123},
			expectedSql:  "SELECT * FROM users WHERE price = '$' OR id = ?",
			expectedArgs: []any{123},
		},
		{
			name:         "$ followed by number (invalid identifier)",
			rawSql:       "SELECT * FROM users WHERE score > $123invalid",
			args:         map[string]any{},
			expectedSql:  "SELECT * FROM users WHERE score > $123invalid",
			expectedArgs: []any{},
		},
		{
			name:         "$ followed by special character",
			rawSql:       "SELECT * FROM users WHERE name != $-invalid",
			args:         map[string]any{},
			expectedSql:  "SELECT * FROM users WHERE name != $-invalid",
			expectedArgs: []any{},
		},
		{
			name:         "parameter at start of string",
			rawSql:       "$id = 123",
			args:         map[string]any{"id": 456},
			expectedSql:  "? = 123",
			expectedArgs: []any{456},
		},
		{
			name:         "parameter at end of string",
			rawSql:       "SELECT * FROM users WHERE id = $id",
			args:         map[string]any{"id": 789},
			expectedSql:  "SELECT * FROM users WHERE id = ?",
			expectedArgs: []any{789},
		},
		{
			name:         "no parameters",
			rawSql:       "SELECT * FROM users",
			args:         map[string]any{},
			expectedSql:  "SELECT * FROM users",
			expectedArgs: []any{},
		},
		{
			name:         "empty string",
			rawSql:       "",
			args:         map[string]any{},
			expectedSql:  "",
			expectedArgs: []any{},
		},
		{
			name:         "single $ character",
			rawSql:       "$",
			args:         map[string]any{},
			expectedSql:  "$",
			expectedArgs: []any{},
		},
		{
			name:         "parameter name starting with underscore",
			rawSql:       "SELECT * FROM users WHERE id = $_private",
			args:         map[string]any{"_private": "secret"},
			expectedSql:  "SELECT * FROM users WHERE id = ?",
			expectedArgs: []any{"secret"},
		},
		{
			name:         "mixed valid and invalid parameters",
			rawSql:       "SELECT * FROM users WHERE id = $id AND price LIKE '$19.99' AND name = $name",
			args:         map[string]any{"id": 123, "name": "John"},
			expectedSql:  "SELECT * FROM users WHERE id = ? AND price LIKE '$19.99' AND name = ?",
			expectedArgs: []any{123, "John"},
		},
		{
			name:         "parameter with various value types",
			rawSql:       "SELECT * FROM mixed WHERE str = $str AND num = $num AND bool = $bool",
			args:         map[string]any{"str": "hello", "num": 42, "bool": true},
			expectedSql:  "SELECT * FROM mixed WHERE str = ? AND num = ? AND bool = ?",
			expectedArgs: []any{"hello", 42, true},
		},
		{
			name:         "nil value parameter",
			rawSql:       "SELECT * FROM users WHERE deleted_at = $deleted",
			args:         map[string]any{"deleted": nil},
			expectedSql:  "SELECT * FROM users WHERE deleted_at = ?",
			expectedArgs: []any{nil},
		},
		{
			name:        "missing parameter should return error",
			rawSql:      "SELECT * FROM users WHERE id = $missing",
			args:        map[string]any{"other": 123},
			shouldError: true,
			errorMsg:    "missing argument for named parameter: missing",
		},
		{
			name:        "multiple missing parameters should return error on first",
			rawSql:      "SELECT * FROM users WHERE id = $missing1 AND name = $missing2",
			args:        map[string]any{},
			shouldError: true,
			errorMsg:    "missing argument for named parameter: missing1",
		},
		{
			name:         "escaped $ at beginning",
			rawSql:       "$$param = 123",
			args:         map[string]any{},
			expectedSql:  "$param = 123",
			expectedArgs: []any{},
		},
		{
			name:         "multiple escaped $ symbols",
			rawSql:       "$$first AND $$second",
			args:         map[string]any{},
			expectedSql:  "$first AND $second",
			expectedArgs: []any{},
		},
		{
			name:         "consecutive $ symbols (triple)",
			rawSql:       "$$$123",
			args:         map[string]any{},
			expectedSql:  "$$123",
			expectedArgs: []any{},
		},
		{
			name:         "escaped $ with valid parameter after",
			rawSql:       "$$literal AND $param",
			args:         map[string]any{"param": "value"},
			expectedSql:  "$literal AND ?",
			expectedArgs: []any{"value"},
		},
		{
			name:         "quadruple $ symbols",
			rawSql:       "$$$$double",
			args:         map[string]any{},
			expectedSql:  "$$double",
			expectedArgs: []any{},
		},
		{
			name:         "parameter followed by punctuation",
			rawSql:       "WHERE id = $id, name = $name;",
			args:         map[string]any{"id": 1, "name": "test"},
			expectedSql:  "WHERE id = ?, name = ?;",
			expectedArgs: []any{1, "test"},
		},
		{
			name:         "unicode characters in SQL",
			rawSql:       "SELECT * FROM üsers WHERE nämé = $name",
			args:         map[string]any{"name": "tëst"},
			expectedSql:  "SELECT * FROM üsers WHERE nämé = ?",
			expectedArgs: []any{"tëst"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualSql, actualArgs, err := db.replaceNames(tt.rawSql, tt.args)

			if tt.shouldError {
				require.Error(t, err)
				require.Equal(t, tt.errorMsg, err.Error())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.expectedSql, actualSql)
			require.Equal(t, tt.expectedArgs, actualArgs)
		})
	}
}

func TestDB_generateSelect(t *testing.T) {
	type TestStruct struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email_address"`
		Age   int    // no db tag, should use snake_case
	}

	db := &DB{}

	model, err := newModelType(TestStruct{}, defaultPluralizer)
	require.NoError(t, err)

	actualSQL, actualFields := db.generateSelect(model)

	expectedSQL := "SELECT `test_structs`.`id`, `test_structs`.`name`, `test_structs`.`email_address`, `test_structs`.`age` FROM test_structs"
	expectedFields := []string{"ID", "Name", "Email", "Age"}

	require.Equal(t, expectedSQL, actualSQL)
	require.Equal(t, expectedFields, actualFields)
}
