package common

import (
	"context"
	"errors"
	"testing"

	"github.com/gofreego/database/mocks"
	sqlpkg "github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_Update(t *testing.T) {
	t.Run("successful update with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Updated Name")).
			Add("email", sqlpkg.NewValue("updated@example.com")).
			Add("is_active", sqlpkg.NewValue(1))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(3))
		values := []any{"Updated Name", "updated@example.com", 1, 123}
		expectedQuery := "UPDATE users SET name = ?, email = ?, is_active = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 1, 2, 3}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery, "Updated Name", "updated@example.com", 1, 123).Return(mockResult, nil)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("successful update with prepared statement", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		updates := sqlpkg.NewUpdates().
			Add("is_active", sqlpkg.NewValue(0))
		condition := sqlpkg.NewCondition("email", sqlpkg.LIKE, sqlpkg.NewIndexedValue(0))
		values := []any{"%inactive%"}
		expectedQuery := "UPDATE users SET is_active = ? WHERE email LIKE ?"
		expectedValueIndexes := []int{0, 0}
		preparedName := "update_users_by_email"

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare not implemented in unit test"))

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values, sqlpkg.Options{PreparedName: preparedName})

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
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("No Match"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{999} // Non-existent ID
		expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with no rows affected
		mockResult := &mockResult{rowsAffected: 0}
		db.On("ExecContext", mock.Anything, expectedQuery, "No Match", 999).Return(mockResult, nil)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected)
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

		table := sqlpkg.NewTable("users")
		updates := sqlpkg.NewUpdates().
			Add("is_active", sqlpkg.NewValue(1))
		condition := sqlpkg.NewCondition("is_active", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{0}
		expectedQuery := "UPDATE users SET is_active = ? WHERE is_active = ?"
		expectedValueIndexes := []int{0, 0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with multiple rows affected
		mockResult := &mockResult{rowsAffected: 5}
		db.On("ExecContext", mock.Anything, expectedQuery, 1, 0).Return(mockResult, nil)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)
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
		updates := sqlpkg.NewUpdates().
			Add("invalid_field", sqlpkg.NewValue("test"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedErr := errors.New("invalid field name")

		// Mock parser to return error
		parser.On("ParseUpdateQuery", table, updates, condition).Return("", nil, expectedErr)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

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
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Error User"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 0}
		expectedErr := errors.New("connection timeout")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, "Error User", 123).Return(nil, expectedErr)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "connection timeout")
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
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Rows Error User"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution with rows affected error
		mockResult := &mockResult{rowsAffectedErr: errors.New("rows affected not supported")}
		db.On("ExecContext", mock.Anything, expectedQuery, "Rows Error User", 123).Return(mockResult, nil)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "rows affected not supported")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("context cancellation", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Context User"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return context cancellation error
		db.On("ExecContext", mock.Anything, expectedQuery, "Context User", 123).Return(nil, context.Canceled)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		rowsAffected, err := executor.Update(ctx, table, updates, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "context canceled")
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
		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Prepare User"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
		expectedValueIndexes := []int{0, 0}
		preparedName := "update_user"
		expectedErr := errors.New("syntax error")

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database prepare to return error
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, expectedErr)

		rowsAffected, err := executor.Update(context.Background(), table, updates, condition, values, sqlpkg.Options{PreparedName: preparedName})

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "syntax error")
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

		updates := sqlpkg.NewUpdates().
			Add("name", sqlpkg.NewValue("Nil Table User"))
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedErr := errors.New("table cannot be nil")

		// Mock parser to return error for nil table
		parser.On("ParseUpdateQuery", (*sqlpkg.Table)(nil), updates, condition).Return("", nil, expectedErr)

		rowsAffected, err := executor.Update(context.Background(), nil, updates, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "table cannot be nil")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("nil updates", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
		values := []any{123}
		expectedErr := errors.New("updates cannot be nil")

		// Mock parser to return error for nil updates
		parser.On("ParseUpdateQuery", table, (*sqlpkg.Updates)(nil), condition).Return("", nil, expectedErr)

		rowsAffected, err := executor.Update(context.Background(), table, nil, condition, values)

		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Contains(t, err.Error(), "updates cannot be nil")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
	})

	t.Run("nil condition (update all)", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		table := sqlpkg.NewTable("users")
		updates := sqlpkg.NewUpdates().
			Add("is_active", sqlpkg.NewValue(1))
		values := []any{}
		expectedQuery := "UPDATE users SET is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseUpdateQuery", table, updates, (*sqlpkg.Condition)(nil)).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 10}
		db.On("ExecContext", mock.Anything, expectedQuery, 1).Return(mockResult, nil)

		rowsAffected, err := executor.Update(context.Background(), table, updates, nil, values)

		assert.NoError(t, err)
		assert.Equal(t, int64(10), rowsAffected)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

func BenchmarkExecutor_Update(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	table := sqlpkg.NewTable("users")
	updates := sqlpkg.NewUpdates().
		Add("name", sqlpkg.NewValue("Benchmark User"))
	condition := sqlpkg.NewCondition("id", sqlpkg.EQ, sqlpkg.NewIndexedValue(0))
	values := []any{123}
	expectedQuery := "UPDATE users SET name = ? WHERE id = ?"
	expectedValueIndexes := []int{0, 0}

	// Mock parser to return the expected query and value indexes
	parser.On("ParseUpdateQuery", table, updates, condition).Return(expectedQuery, expectedValueIndexes, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, "Benchmark User", 123).Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = executor.Update(context.Background(), table, updates, condition, values)
	}
}
