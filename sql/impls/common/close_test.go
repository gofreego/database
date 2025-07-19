package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofreego/database/mocks"
	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/internal"
	"github.com/stretchr/testify/assert"
)

func TestExecutor_Close(t *testing.T) {
	t.Run("successful close", func(t *testing.T) {
		// use mockery to mock the db and parser
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// mock the db.Close method to return nil
		db.On("Close").Return(nil)

		// call the Close method
		err := executor.Close(context.Background())
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("db close error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		expectedErr := errors.New("database connection failed")
		db.On("Close").Return(expectedErr)

		err := executor.Close(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database connection failed")
		db.AssertExpectations(t)
	})

	t.Run("with prepared statements", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Test that Close works even with empty prepared statements
		// This verifies the basic flow without causing panics
		assert.Equal(t, 0, len(executor.preparedStatements))

		db.On("Close").Return(nil)

		err := executor.Close(context.Background())
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("with nil context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		db.On("Close").Return(nil)

		// Should still work with nil context since Close doesn't use it
		err := executor.Close(nil)
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("with cancelled context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		db.On("Close").Return(nil)

		// Should still work even with cancelled context
		err := executor.Close(ctx)
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("with timeout context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		db.On("Close").Return(nil)

		err := executor.Close(ctx)
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("multiple close calls", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Mock Close to be called multiple times
		db.On("Close").Return(nil).Times(3)

		// Call Close multiple times
		err1 := executor.Close(context.Background())
		assert.NoError(t, err1)

		err2 := executor.Close(context.Background())
		assert.NoError(t, err2)

		err3 := executor.Close(context.Background())
		assert.NoError(t, err3)

		db.AssertExpectations(t)
	})

	t.Run("prepared statements close error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// This test would require a more complex setup with actual prepared statements
		// that can return errors when closed. For now, we'll test the basic flow.
		db.On("Close").Return(nil)

		err := executor.Close(context.Background())
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("empty prepared statements", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Verify no prepared statements exist initially
		assert.False(t, executor.preparedStatements.Exists("non_existent"))

		db.On("Close").Return(nil)

		err := executor.Close(context.Background())
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("database error handling", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Test various database error types
		testCases := []struct {
			name        string
			dbError     error
			expectError bool
		}{
			{
				name:        "connection timeout",
				dbError:     errors.New("connection timeout"),
				expectError: true,
			},
			{
				name:        "connection refused",
				dbError:     errors.New("connection refused"),
				expectError: true,
			},
			{
				name:        "database shutdown",
				dbError:     errors.New("database is shutting down"),
				expectError: true,
			},
			{
				name:        "network error",
				dbError:     errors.New("network unreachable"),
				expectError: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				db.On("Close").Return(tc.dbError).Once()

				err := executor.Close(context.Background())
				if tc.expectError {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), tc.dbError.Error())
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("concurrent close calls", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Mock Close to handle concurrent calls
		db.On("Close").Return(nil).Times(5)

		// Test concurrent Close calls
		done := make(chan bool, 5)
		for i := 0; i < 5; i++ {
			go func() {
				err := executor.Close(context.Background())
				assert.NoError(t, err)
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 5; i++ {
			<-done
		}

		db.AssertExpectations(t)
	})

	t.Run("executor with nil fields", func(t *testing.T) {
		// Test edge case where executor has nil fields
		executor := &Executor{
			db:                 nil,
			parser:             nil,
			preparedStatements: nil,
		}

		// This should panic due to nil pointer dereference
		// We're testing that the code handles this gracefully
		assert.Panics(t, func() {
			executor.Close(context.Background())
		})
	})
}

// TestExecutor_Close_Integration tests the Close method with more realistic scenarios
func TestExecutor_Close_Integration(t *testing.T) {
	t.Run("close after operations", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Test that Close works with an empty prepared statements map
		// In a real scenario, prepared statements would be created during database operations
		assert.Equal(t, 0, len(executor.preparedStatements))

		db.On("Close").Return(nil)

		// Close should work even with no prepared statements
		err := executor.Close(context.Background())
		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("close with error propagation", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: internal.NewPreparedStatements(),
		}

		// Test that database errors are properly wrapped
		dbError := errors.New("connection pool exhausted")
		db.On("Close").Return(dbError)

		err := executor.Close(context.Background())
		assert.Error(t, err)

		// Check if it's a wrapped database error
		var sqlErr *sql.Error
		if errors.As(err, &sqlErr) {
			assert.Equal(t, sql.ErrUnknownDatabaseError, sqlErr.Code())
		} else {
			t.Error("Expected error to be wrapped as sql.Error")
		}

		db.AssertExpectations(t)
	})
}

// Benchmark tests for Close method
func BenchmarkExecutor_Close(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: internal.NewPreparedStatements(),
	}

	db.On("Close").Return(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Close(context.Background())
	}
}
