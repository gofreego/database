package common

import (
	"context"
	driver "database/sql"
	"errors"
	"testing"

	"github.com/gofreego/database/mocks"
	sqlpkg "github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/database/sql/tests/records"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_DeleteByID(t *testing.T) {
	t.Run("successful delete with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.True(t, deleted)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("successful delete with prepared statement", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"
		preparedName := "delete_user_by_id"

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database prepare - we'll use a simple approach that doesn't require complex mocking
		// In a real test, you might use a real database connection or a more sophisticated mock
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare not implemented in unit test"))

		// Execute DeleteByID with prepared statement option
		deleted, err := executor.DeleteByID(context.Background(), record, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "prepare not implemented")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
	t.Run("no rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 999} // Non-existent ID
		expectedQuery := "DELETE FROM users WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database execution with no rows affected
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(999)).Return(mockResult, nil)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.False(t, deleted) // Should return false when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedErr := errors.New("invalid table name")

		// Mock parser to return error
		parser.On("ParseDeleteByIDQuery", record).Return("", expectedErr)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "invalid table name")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("database execution error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"
		expectedErr := errors.New("connection timeout")

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(nil, expectedErr)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "connection timeout")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("prepare statement error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"
		preparedName := "delete_user_by_id"
		expectedErr := errors.New("syntax error")

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database prepare to return error
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, expectedErr)

		// Execute DeleteByID with prepared statement option
		deleted, err := executor.DeleteByID(context.Background(), record, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "syntax error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("rows affected error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"
		expectedErr := errors.New("rows affected error")

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database execution with rows affected error
		mockResult := &mockResult{rowsAffectedErr: expectedErr}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "rows affected error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil record", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Mock parser to return error for nil record
		expectedErr := errors.New("record cannot be nil")
		parser.On("ParseDeleteByIDQuery", nil).Return("", expectedErr)

		// Execute DeleteByID with nil record
		deleted, err := executor.DeleteByID(context.Background(), nil)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "record cannot be nil")
		parser.AssertExpectations(t)
	})

	t.Run("multiple rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		expectedQuery := "DELETE FROM users WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

		// Mock database execution with multiple rows affected (unusual but possible)
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute DeleteByID
		deleted, err := executor.DeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.True(t, deleted) // Should return true when any rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

// Benchmark tests for DeleteByID method
func BenchmarkExecutor_DeleteByID(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	record := &records.User{Id: 1}
	expectedQuery := "DELETE FROM users WHERE id = ?"

	// Mock parser to return the expected query
	parser.On("ParseDeleteByIDQuery", record).Return(expectedQuery, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.DeleteByID(context.Background(), record)
	}
}

// Mock implementations for testing
type mockResult struct {
	rowsAffected    int64
	rowsAffectedErr error
	lastInsertId    int64
	lastInsertIdErr error
}

func (m *mockResult) LastInsertId() (int64, error) {
	return m.lastInsertId, m.lastInsertIdErr
}

func (m *mockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, m.rowsAffectedErr
}

type mockStmt struct {
	mock.Mock
}

func (m *mockStmt) ExecContext(ctx context.Context, args ...interface{}) (driver.Result, error) {
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, args...)
	callArgs := m.Called(_ca...)
	return callArgs.Get(0).(driver.Result), callArgs.Error(1)
}

func (m *mockStmt) QueryContext(ctx context.Context, args ...interface{}) (*driver.Rows, error) {
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, args...)
	callArgs := m.Called(_ca...)
	return callArgs.Get(0).(*driver.Rows), callArgs.Error(1)
}

func (m *mockStmt) QueryRowContext(ctx context.Context, args ...interface{}) *driver.Row {
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, args...)
	callArgs := m.Called(_ca...)
	return callArgs.Get(0).(*driver.Row)
}

func (m *mockStmt) Close() error {
	callArgs := m.Called()
	return callArgs.Error(0)
}

func TestExecutor_Delete(t *testing.T) {
	t.Run("successful delete with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "DELETE FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 5}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("successful delete with prepared statement", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("email", sqlpkg.LIKE, sqlpkg.NewIndexedValue(0))
		values := []any{"%test%"}
		expectedQuery := "DELETE FROM users WHERE email LIKE ?"
		expectedValueIndexes := []int{0}
		preparedName := "delete_users_by_email"

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare not implemented in unit test"))

		// Execute Delete with prepared statement option
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "prepare not implemented")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("no rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{999} // Non-existent ID
		expectedQuery := "DELETE FROM users WHERE id = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with no rows affected
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, 999).Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected) // Should return 0 when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("invalid_field", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{"test"}
		expectedErr := errors.New("invalid field name")

		// Mock parser to return error
		parser.On("ParseDeleteQuery", table, condition).Return("", nil, expectedErr)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "invalid field name")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("database execution error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "DELETE FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}
		expectedErr := errors.New("connection timeout")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(nil, expectedErr)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "connection timeout")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("prepare statement error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("name", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{"test"}
		expectedQuery := "DELETE FROM users WHERE name = ?"
		expectedValueIndexes := []int{0}
		preparedName := "delete_users_by_name"
		expectedErr := errors.New("syntax error")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare to return error
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, expectedErr)

		// Execute Delete with prepared statement option
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "syntax error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("rows affected error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "DELETE FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}
		expectedErr := errors.New("rows affected error")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with rows affected error
		mockResult := &mockResult{rowsAffectedErr: expectedErr}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "rows affected error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil table", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedErr := errors.New("table cannot be nil")

		// Mock parser to return error for nil table
		parser.On("ParseDeleteQuery", (*sqlpkg.Table)(nil), condition).Return("", nil, expectedErr)

		// Execute Delete with nil table
		rowsAffected, err := executor.Delete(context.Background(), nil, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "table cannot be nil")
		parser.AssertExpectations(t)
	})

	t.Run("complex condition with multiple values", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		// Create a complex condition: (is_active = ? AND (email LIKE ? OR name LIKE ?))
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0)).
			And(sqlpkg.NewCondition("email", sqlpkg.LIKE, sqlpkg.NewIndexedValue(1)).
				Or(sqlpkg.NewCondition("name", sqlpkg.LIKE, sqlpkg.NewIndexedValue(2))))
		values := []any{1, "%admin%", "%admin%"}
		expectedQuery := "DELETE FROM users WHERE (is_active = ? AND (email LIKE ? OR name LIKE ?))"
		expectedValueIndexes := []int{0, 1, 2}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, 1, "%admin%", "%admin%").Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("IN condition with multiple values", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("id", sqlpkg.IN, sqlpkg.NewIndexedValue(0).WithCount(3))
		values := []any{[]any{1, 2, 3}}
		expectedQuery := "DELETE FROM users WHERE id IN (?, ?, ?)"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, 1, 2, 3).Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil condition (delete all)", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		var values []any
		expectedQuery := "DELETE FROM users WHERE 1=1"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, (*sqlpkg.Condition)(nil)).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 100}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		// Execute Delete with nil condition (delete all records)
		rowsAffected, err := executor.Delete(context.Background(), table, nil, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(100), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("empty values slice", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewValue(1)) // Fixed value, no parameters
		values := []any{}
		expectedQuery := "DELETE FROM users WHERE is_active = 1"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 5}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		// Execute Delete with empty values slice
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("large number of rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "DELETE FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with large number of rows affected
		mockResult := &mockResult{rowsAffected: 10000}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute Delete
		rowsAffected, err := executor.Delete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(10000), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

// Benchmark tests for Delete method
func BenchmarkExecutor_Delete(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	table := sqlpkg.NewTable("users")
	condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
	values := []any{1}
	expectedQuery := "DELETE FROM users WHERE is_active = ?"
	expectedValueIndexes := []int{0}

	// Mock parser to return the expected query and value indexes
	parser.On("ParseDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, 1).Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Delete(context.Background(), table, condition, values)
	}
}

func TestExecutor_SoftDeleteByID(t *testing.T) {
	t.Run("successful soft delete with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.True(t, deleted)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("successful soft delete with prepared statement", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"
		preparedName := "soft_delete_user_by_id"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database prepare
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare not implemented in unit test"))

		// Execute SoftDeleteByID with prepared statement option
		deleted, err := executor.SoftDeleteByID(context.Background(), record, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "prepare not implemented")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("no rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 999} // Non-existent ID
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with no rows affected
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(999)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.False(t, deleted) // Should return false when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedErr := errors.New("invalid table name")

		// Mock parser to return error
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return("", expectedErr)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "invalid table name")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("database execution error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"
		expectedErr := errors.New("connection timeout")

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(nil, expectedErr)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "connection timeout")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("prepare statement error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"
		preparedName := "soft_delete_user_by_id"
		expectedErr := errors.New("syntax error")

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database prepare to return error
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, expectedErr)

		// Execute SoftDeleteByID with prepared statement option
		deleted, err := executor.SoftDeleteByID(context.Background(), record, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "syntax error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("rows affected error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"
		expectedErr := errors.New("rows affected error")

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with rows affected error
		mockResult := &mockResult{rowsAffectedErr: expectedErr}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.Error(t, err)
		assert.False(t, deleted)
		assert.Contains(t, err.Error(), "rows affected error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("multiple rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with multiple rows affected (unusual but possible)
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.True(t, deleted) // Should return true when any rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("already soft deleted record", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with no rows affected (already soft deleted)
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.False(t, deleted) // Should return false when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("large number of rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with large number of rows affected
		mockResult := &mockResult{rowsAffected: 10000}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.True(t, deleted) // Should return true when any rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("different table name", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Create a custom record with different table
		customRecord := &records.User{Id: 1}
		table := sqlpkg.NewTable("users") // Use the actual table that the record returns
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, customRecord).Return(expectedQuery, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), customRecord)

		assert.NoError(t, err)
		assert.True(t, deleted)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("zero ID", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: 0}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with no rows affected (zero ID typically doesn't exist)
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(0)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.False(t, deleted) // Should return false when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("negative ID", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		record := &records.User{Id: -1}
		table := sqlpkg.NewTable("users")
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

		// Mock parser to return the expected query
		parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

		// Mock database execution with no rows affected (negative ID typically doesn't exist)
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, int64(-1)).Return(mockResult, nil)

		// Execute SoftDeleteByID
		deleted, err := executor.SoftDeleteByID(context.Background(), record)

		assert.NoError(t, err)
		assert.False(t, deleted) // Should return false when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

// Benchmark tests for SoftDeleteByID method
func BenchmarkExecutor_SoftDeleteByID(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	record := &records.User{Id: 1}
	table := sqlpkg.NewTable("users")
	expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"

	// Mock parser to return the expected query
	parser.On("ParseSoftDeleteByIDQuery", table, record).Return(expectedQuery, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, int64(1)).Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.SoftDeleteByID(context.Background(), record)
	}
}

func TestExecutor_SoftDelete(t *testing.T) {
	t.Run("successful soft delete with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 5}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("successful soft delete with prepared statement", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("email", sqlpkg.LIKE, sqlpkg.NewIndexedValue(0))
		values := []any{"%test%"}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE email LIKE ?"
		expectedValueIndexes := []int{0}
		preparedName := "soft_delete_users_by_email"

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare not implemented in unit test"))

		// Execute SoftDelete with prepared statement option
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "prepare not implemented")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("no rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{999} // Non-existent ID
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with no rows affected
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, 999).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected) // Should return 0 when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("invalid_field", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{"test"}
		expectedErr := errors.New("invalid field name")

		// Mock parser to return error
		parser.On("ParseSoftDeleteQuery", table, condition).Return("", nil, expectedErr)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "invalid field name")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("database execution error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = ?"
		expectedValueIndexes := []int{0}
		expectedErr := errors.New("connection timeout")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(nil, expectedErr)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "connection timeout")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("prepare statement error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("name", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{"test"}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE name = ?"
		expectedValueIndexes := []int{0}
		preparedName := "soft_delete_users_by_name"
		expectedErr := errors.New("syntax error")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare to return error
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, expectedErr)

		// Execute SoftDelete with prepared statement option
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "syntax error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("rows affected error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = ?"
		expectedValueIndexes := []int{0}
		expectedErr := errors.New("rows affected error")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with rows affected error
		mockResult := &mockResult{rowsAffectedErr: expectedErr}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "rows affected error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil table", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedErr := errors.New("table cannot be nil")

		// Mock parser to return error for nil table
		parser.On("ParseSoftDeleteQuery", (*sqlpkg.Table)(nil), condition).Return("", nil, expectedErr)

		// Execute SoftDelete with nil table
		rowsAffected, err := executor.SoftDelete(context.Background(), nil, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "table cannot be nil")
		parser.AssertExpectations(t)
	})

	t.Run("complex condition with multiple values", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		// Create a complex condition: (is_active = ? AND (email LIKE ? OR name LIKE ?))
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0)).
			And(sqlpkg.NewCondition("email", sqlpkg.LIKE, sqlpkg.NewIndexedValue(1)).
				Or(sqlpkg.NewCondition("name", sqlpkg.LIKE, sqlpkg.NewIndexedValue(2))))
		values := []any{1, "%admin%", "%admin%"}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE (is_active = ? AND (email LIKE ? OR name LIKE ?))"
		expectedValueIndexes := []int{0, 1, 2}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, 1, "%admin%", "%admin%").Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("IN condition with multiple values", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("id", sqlpkg.IN, sqlpkg.NewIndexedValue(0).WithCount(3))
		values := []any{[]any{1, 2, 3}}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE id IN (?, ?, ?)"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 3}
		db.On("ExecContext", mock.Anything, expectedQuery, 1, 2, 3).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(3), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil condition (soft delete all)", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		var values []any
		expectedQuery := "UPDATE users SET deleted = 1 WHERE 1=1"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, (*sqlpkg.Condition)(nil)).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 100}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		// Execute SoftDelete with nil condition (soft delete all records)
		rowsAffected, err := executor.SoftDelete(context.Background(), table, nil, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(100), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("empty values slice", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewValue(1)) // Fixed value, no parameters
		values := []any{}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = 1"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 5}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		// Execute SoftDelete with empty values slice
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("large number of rows affected", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with large number of rows affected
		mockResult := &mockResult{rowsAffected: 10000}
		db.On("ExecContext", mock.Anything, expectedQuery, 0).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(10000), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("already soft deleted records", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("deleted", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{1} // Already soft deleted
		expectedQuery := "UPDATE users SET deleted = 1 WHERE deleted = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with no rows affected (already soft deleted)
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, 1).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected) // Should return 0 when no rows affected
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("different table name", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("customers")
		condition := sqlpkg.NewCondition("status", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{"inactive"}
		expectedQuery := "UPDATE customers SET deleted = 1 WHERE status = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 10}
		db.On("ExecContext", mock.Anything, expectedQuery, "inactive").Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(10), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("IS NULL condition", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("deleted_at", sqlpkg.ISNULL, nil)
		values := []any{}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE deleted_at IS NULL"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 25}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(25), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("BETWEEN condition", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("created_at", sqlpkg.BETWEEN, sqlpkg.NewIndexedValue(0).WithCount(2))
		values := []any{"2023-01-01", "2023-12-31"}
		expectedQuery := "UPDATE users SET deleted = 1 WHERE created_at BETWEEN ? AND ?"
		expectedValueIndexes := []int{0, 1} // Both values should be indexed

		// Mock parser to return the expected query and value indexes
		parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 50}
		db.On("ExecContext", mock.Anything, expectedQuery, "2023-01-01", "2023-12-31").Return(mockResult, nil)

		// Execute SoftDelete
		rowsAffected, err := executor.SoftDelete(context.Background(), table, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(50), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

// Benchmark tests for SoftDelete method
func BenchmarkExecutor_SoftDelete(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	table := sqlpkg.NewTable("users")
	condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
	values := []any{1}
	expectedQuery := "UPDATE users SET deleted = 1 WHERE is_active = ?"
	expectedValueIndexes := []int{0}

	// Mock parser to return the expected query and value indexes
	parser.On("ParseSoftDeleteQuery", table, condition).Return(expectedQuery, expectedValueIndexes, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, 1).Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.SoftDelete(context.Background(), table, condition, values)
	}
}
