package common

import (
	"context"
	"errors"
	"testing"

	"github.com/gofreego/database/mocks"
	"github.com/gofreego/database/sql/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_Insert(t *testing.T) {
	t.Run("successful insert with direct query", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)
		record := mocks.NewRecord(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedQuery := "INSERT INTO users (name, email, age) VALUES (?, ?, ?)"
		expectedValues := []any{"John Doe", "john@example.com", 30}
		expectedLastInsertID := int64(123)

		// Mock record to return values
		record.On("Values").Return(expectedValues)
		record.On("SetID", expectedLastInsertID).Return()

		// Mock parser to return the expected query and values
		parser.On("ParseInsertQuery", record).Return(expectedQuery, expectedValues, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery, "John Doe", "john@example.com", 30).Return(mockResult, nil)

		err := executor.Insert(context.Background(), record)

		assert.NoError(t, err)
		record.AssertExpectations(t)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)
		record := mocks.NewRecord(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedErr := errors.New("invalid table name")

		// Mock parser to return error
		parser.On("ParseInsertQuery", record).Return("", nil, expectedErr)

		err := executor.Insert(context.Background(), record)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid table name")
		parser.AssertExpectations(t)
		// Database should not be called when parser fails
		db.AssertNotCalled(t, "ExecContext")
		record.AssertNotCalled(t, "SetID")
	})

	t.Run("database execution error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)
		record := mocks.NewRecord(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedQuery := "INSERT INTO users (name) VALUES (?)"
		expectedValues := []any{"Error User"}
		expectedErr := errors.New("connection timeout")

		// Mock record to return values
		record.On("Values").Return(expectedValues)

		// Mock parser to return the expected query and values
		parser.On("ParseInsertQuery", record).Return(expectedQuery, expectedValues, nil)

		// Mock database execution to return error
		db.On("ExecContext", mock.Anything, expectedQuery, "Error User").Return(nil, expectedErr)

		err := executor.Insert(context.Background(), record)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection timeout")
		record.AssertExpectations(t)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
		record.AssertNotCalled(t, "SetID")
	})

	t.Run("LastInsertId error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)
		record := mocks.NewRecord(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedQuery := "INSERT INTO users (name) VALUES (?)"
		expectedValues := []any{"Test User"}
		expectedErr := errors.New("LastInsertId not supported")

		// Mock record to return values
		record.On("Values").Return(expectedValues)

		// Mock parser to return the expected query and values
		parser.On("ParseInsertQuery", record).Return(expectedQuery, expectedValues, nil)

		// Mock database execution with LastInsertId error
		mockResult := &mockResult{rowsAffectedErr: expectedErr}
		db.On("ExecContext", mock.Anything, expectedQuery, "Test User").Return(mockResult, nil)

		err := executor.Insert(context.Background(), record)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LastInsertId not supported")
		record.AssertExpectations(t)
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
		record.AssertNotCalled(t, "SetID")
	})

	t.Run("empty values", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)
		record := mocks.NewRecord(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedQuery := "INSERT INTO users () VALUES ()"
		expectedValues := []any{}
		expectedLastInsertID := int64(123)

		// Mock record to return empty values
		record.On("Values").Return(expectedValues)
		record.On("SetID", expectedLastInsertID).Return()

		// Mock parser to return the expected query and values
		parser.On("ParseInsertQuery", record).Return(expectedQuery, expectedValues, nil)

		// Mock database execution
		mockResult := &mockResult{rowsAffected: 1}
		db.On("ExecContext", mock.Anything, expectedQuery).Return(mockResult, nil)

		err := executor.Insert(context.Background(), record)

		assert.NoError(t, err)
		record.AssertExpectations(t)
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

		err := executor.Insert(context.Background(), nil)

		assert.Error(t, err)
		// Database and parser should not be called with nil record
		db.AssertNotCalled(t, "ExecContext")
		parser.AssertNotCalled(t, "ParseInsertQuery")
	})
}

func BenchmarkExecutor_Insert(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)
	record := mocks.NewRecord(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	expectedQuery := "INSERT INTO users (name, email) VALUES (?, ?)"
	expectedValues := []any{"Bench User", "bench@example.com"}

	// Mock record to return values
	record.On("Values").Return(expectedValues)
	record.On("SetID", int64(1)).Return()

	// Mock parser to return the expected query and values
	parser.On("ParseInsertQuery", record).Return(expectedQuery, expectedValues, nil)

	// Mock database execution
	mockResult := &mockResult{rowsAffected: 1}
	db.On("ExecContext", mock.Anything, expectedQuery, "Bench User", "bench@example.com").Return(mockResult, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Insert(context.Background(), record)
	}
}
