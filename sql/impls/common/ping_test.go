package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofreego/database/mocks"
	"github.com/gofreego/database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestExecutor_Ping(t *testing.T) {
	t.Run("successful ping", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		// Mock database to return success
		db.On("PingContext", mock.Anything).Return(nil)

		err := executor.Ping(context.Background())

		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("database ping error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("connection refused")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "connection refused")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("context cancellation", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		db.On("PingContext", ctx).Return(context.Canceled)

		err := executor.Ping(ctx)

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "context canceled")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("context timeout", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		// Wait for timeout
		time.Sleep(2 * time.Millisecond)

		db.On("PingContext", ctx).Return(context.DeadlineExceeded)

		err := executor.Ping(ctx)

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("nil context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		// Mock database to handle nil context
		db.On("PingContext", nil).Return(errors.New("nil context"))

		err := executor.Ping(nil)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil context")
		db.AssertExpectations(t)
	})

	t.Run("connection timeout error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("connection timeout")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "connection timeout")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("database shutdown error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("database shutdown")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "database shutdown")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("network error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("network unreachable")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "network unreachable")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("authentication error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("authentication failed")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "authentication failed")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("permission denied error", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		expectedErr := errors.New("permission denied")
		db.On("PingContext", mock.Anything).Return(expectedErr)

		err := executor.Ping(context.Background())

		assert.Error(t, err)
		assert.IsType(t, &sql.Error{}, err)
		assert.Contains(t, err.Error(), "permission denied")
		assert.Equal(t, sql.ErrUnknownDatabaseError, err.(*sql.Error).Code())
		db.AssertExpectations(t)
	})

	t.Run("executor with nil db", func(t *testing.T) {
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 nil,
			parser:             parser,
			preparedStatements: nil,
		}

		// This would panic in real code, but we test the error handling
		// In a real scenario, the executor should not be created with nil db
		assert.Panics(t, func() {
			_ = executor.Ping(context.Background())
		})
	})

	t.Run("concurrent ping calls", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		// Mock database to return success for multiple calls
		db.On("PingContext", mock.Anything).Return(nil).Times(5)

		done := make(chan bool, 5)
		for i := 0; i < 5; i++ {
			go func() {
				err := executor.Ping(context.Background())
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

	t.Run("ping with background context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		db.On("PingContext", context.Background()).Return(nil)

		err := executor.Ping(context.Background())

		assert.NoError(t, err)
		db.AssertExpectations(t)
	})

	t.Run("ping with custom context", func(t *testing.T) {
		db := mocks.NewDB(t)
		parser := mocks.NewParser(t)

		executor := &Executor{
			db:                 db,
			parser:             parser,
			preparedStatements: nil,
		}

		ctx := context.WithValue(context.Background(), "test-key", "test-value")
		db.On("PingContext", ctx).Return(nil)

		err := executor.Ping(ctx)

		assert.NoError(t, err)
		db.AssertExpectations(t)
	})
}

func BenchmarkExecutor_Ping(b *testing.B) {
	db := mocks.NewDB(b)
	parser := mocks.NewParser(b)

	executor := &Executor{
		db:                 db,
		parser:             parser,
		preparedStatements: nil,
	}

	// Mock database to return success
	db.On("PingContext", mock.Anything).Return(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = executor.Ping(context.Background())
	}
}
