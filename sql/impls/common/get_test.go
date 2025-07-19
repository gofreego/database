package common

import (
	"context"
	"errors"
	"testing"

	"github.com/gofreego/database/mocks"
	sqlpkg "github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/gofreego/database/sql/tests/records"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_Get(t *testing.T) {
	t.Run("parser error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "invalid_field",
				Operator: sqlpkg.EQ,
				Value:    sqlpkg.NewIndexedValue(0),
			},
		}
		values := []any{1}
		records := &records.Users{}

		// Mock parser to return error
		parser.On("ParseGetByFilterQuery", filter, records).Return("", nil, errors.New("invalid field"))

		err := executor.Get(context.Background(), filter, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid field")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("database query error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "is_active",
				Operator: sqlpkg.EQ,
				Value:    sqlpkg.NewIndexedValue(0),
			},
		}
		values := []any{1}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return error
		db.On("QueryContext", mock.Anything, expectedQuery, 1).Return(nil, errors.New("database error"))

		err := executor.Get(context.Background(), filter, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("prepared statement error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "email",
				Operator: sqlpkg.LIKE,
				Value:    sqlpkg.NewIndexedValue(0),
			},
		}
		values := []any{"%@example.com"}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE email LIKE ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return error on prepare
		db.On("PrepareContext", mock.Anything, expectedQuery).Return(nil, errors.New("prepare error"))

		err := executor.Get(context.Background(), filter, values, records, sqlpkg.Options{PreparedName: "test_prepared"})

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "prepare error")
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

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "is_active",
				Operator: sqlpkg.EQ,
				Value:    sqlpkg.NewIndexedValue(0),
			},
		}
		values := []any{1}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE is_active = ?"
		expectedValueIndexes := []int{0}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return context cancellation error
		db.On("QueryContext", mock.Anything, expectedQuery, 1).Return(nil, context.Canceled)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := executor.Get(ctx, filter, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil filter", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		values := []any{}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", (*sqlpkg.Filter)(nil), records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return error to avoid complex rows mocking
		db.On("QueryContext", mock.Anything, expectedQuery).Return(nil, errors.New("database error"))

		err := executor.Get(context.Background(), nil, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("complex filter with multiple conditions", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Operator: sqlpkg.AND,
				Conditions: []sqlpkg.Condition{
					{
						Field:    "is_active",
						Operator: sqlpkg.EQ,
						Value:    sqlpkg.NewIndexedValue(0),
					},
					{
						Field:    "created_at",
						Operator: sqlpkg.GT,
						Value:    sqlpkg.NewIndexedValue(1),
					},
				},
			},
			Sort:   sqlpkg.NewSort().Add("created_at", sqlpkg.Desc),
			Limit:  sqlpkg.NewValue(int64(10)),
			Offset: sqlpkg.NewIndexedValue(2),
		}
		values := []any{1, int64(1640995200000), int64(0)}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE (is_active = ? AND created_at > ?) ORDER BY created_at DESC LIMIT 10 OFFSET ?"
		expectedValueIndexes := []int{0, 1, 2}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return error to avoid complex rows mocking
		db.On("QueryContext", mock.Anything, expectedQuery, 1, int64(1640995200000), int64(0)).Return(nil, errors.New("database error"))

		err := executor.Get(context.Background(), filter, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
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

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "is_active",
				Operator: sqlpkg.EQ,
				Value:    sqlpkg.NewValue(1), // Fixed value, no index needed
			},
		}
		values := []any{}
		records := &records.Users{}
		expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE is_active = 1"
		expectedValueIndexes := []int{}

		// Mock parser to return the expected query and value indexes
		parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

		// Mock database to return error to avoid complex rows mocking
		db.On("QueryContext", mock.Anything, expectedQuery).Return(nil, errors.New("database error"))

		err := executor.Get(context.Background(), filter, values, records)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database error")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})

	t.Run("nil records", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		filter := &sqlpkg.Filter{
			Condition: &sqlpkg.Condition{
				Field:    "is_active",
				Operator: sqlpkg.EQ,
				Value:    sqlpkg.NewIndexedValue(0),
			},
		}
		values := []any{1}

		// Mock parser to return error for nil records
		parser.On("ParseGetByFilterQuery", filter, nil).Return("", nil, errors.New("records cannot be nil"))

		err := executor.Get(context.Background(), filter, values, nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "records cannot be nil")
		db.AssertExpectations(t)
		parser.AssertExpectations(t)
	})
}

func BenchmarkExecutor_Get(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	filter := &sqlpkg.Filter{
		Condition: &sqlpkg.Condition{
			Field:    "is_active",
			Operator: sqlpkg.EQ,
			Value:    sqlpkg.NewIndexedValue(0),
		},
	}
	values := []any{1}
	records := &records.Users{}
	expectedQuery := "SELECT id, name, email, password_hash, score, is_active, created_at, updated_at FROM users WHERE is_active = ?"
	expectedValueIndexes := []int{0}

	// Mock parser to return the expected query and value indexes
	parser.On("ParseGetByFilterQuery", filter, records).Return(expectedQuery, expectedValueIndexes, nil)

	// Mock database to return error to avoid complex rows mocking
	db.On("QueryContext", mock.Anything, expectedQuery, 1).Return(nil, errors.New("database error"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = executor.Get(context.Background(), filter, values, records)
	}
}
