package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

/*
Note: please make sure the database is running before running the test
use `make setup-db` to start the database
*/

func TestInsertMany(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql insert many users",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MigrationUP(tt.args.ctx, tt.args.config); err != nil {
				t.Errorf("MigrationUP() failed: %v", err)
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}()

			conn, err := sqlfactory.NewDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test case 1: Insert multiple users successfully
			users := []sql.Record{
				&records.User{
					Name:         "Alice Johnson",
					Email:        "alice@example.com",
					PasswordHash: "hash123",
					IsActive:     1,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				},
				&records.User{
					Name:         "Bob Smith",
					Email:        "bob@example.com",
					PasswordHash: "hash456",
					IsActive:     0,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				},
				&records.User{
					Name:         "Charlie Brown",
					Email:        "charlie@example.com",
					PasswordHash: "hash789",
					IsActive:     1,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				},
			}

			rowsAffected, err := conn.InsertMany(tt.args.ctx, users)
			if err != nil {
				t.Errorf("InsertMany() failed: %v", err)
				return
			}

			if rowsAffected != 3 {
				t.Errorf("InsertMany() failed: expected 3 rows affected, got %d", rowsAffected)
				return
			}

			// Verify the inserted data
			allUsers := &records.Users{}
			getFilter := &sql.Filter{
				Sort: sql.NewSort().Add("id", sql.Asc),
			}
			if err := conn.Get(tt.args.ctx, getFilter, []any{}, allUsers); err != nil {
				t.Errorf("Get() failed: %v", err)
				return
			}

			if len(allUsers.Users) != 3 {
				t.Errorf("Get() failed: expected 3 users, got %d", len(allUsers.Users))
				return
			}

			// Verify first user
			if allUsers.Users[0].Name != "Alice Johnson" {
				t.Errorf("Get() failed: expected name 'Alice Johnson', got '%s'", allUsers.Users[0].Name)
			}
			if allUsers.Users[0].Email != "alice@example.com" {
				t.Errorf("Get() failed: expected email 'alice@example.com', got '%s'", allUsers.Users[0].Email)
			}
			if allUsers.Users[0].IsActive != 1 {
				t.Errorf("Get() failed: expected is_active 1, got %d", allUsers.Users[0].IsActive)
			}

			// Verify second user
			if allUsers.Users[1].Name != "Bob Smith" {
				t.Errorf("Get() failed: expected name 'Bob Smith', got '%s'", allUsers.Users[1].Name)
			}
			if allUsers.Users[1].Email != "bob@example.com" {
				t.Errorf("Get() failed: expected email 'bob@example.com', got '%s'", allUsers.Users[1].Email)
			}
			if allUsers.Users[1].IsActive != 0 {
				t.Errorf("Get() failed: expected is_active 0, got %d", allUsers.Users[1].IsActive)
			}

			// Verify third user
			if allUsers.Users[2].Name != "Charlie Brown" {
				t.Errorf("Get() failed: expected name 'Charlie Brown', got '%s'", allUsers.Users[2].Name)
			}
			if allUsers.Users[2].Email != "charlie@example.com" {
				t.Errorf("Get() failed: expected email 'charlie@example.com', got '%s'", allUsers.Users[2].Email)
			}
			if allUsers.Users[2].IsActive != 1 {
				t.Errorf("Get() failed: expected is_active 1, got %d", allUsers.Users[2].IsActive)
			}

			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}

func TestInsertManyEmptyRecords(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql insert many empty records",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MigrationUP(tt.args.ctx, tt.args.config); err != nil {
				t.Errorf("MigrationUP() failed: %v", err)
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}()

			conn, err := sqlfactory.NewDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test case: Insert empty slice of records
			emptyUsers := []sql.Record{}
			rowsAffected, err := conn.InsertMany(tt.args.ctx, emptyUsers)
			if err != nil {
				t.Errorf("InsertMany() with empty records failed: %v", err)
				return
			}

			if rowsAffected != 0 {
				t.Errorf("InsertMany() with empty records failed: expected 0 rows affected, got %d", rowsAffected)
				return
			}

			// Verify no records were inserted
			allUsers := &records.Users{}
			getFilter := &sql.Filter{}
			if err := conn.Get(tt.args.ctx, getFilter, []any{}, allUsers); err != nil {
				t.Errorf("Get() failed: %v", err)
				return
			}

			if len(allUsers.Users) != 0 {
				t.Errorf("Get() failed: expected 0 users, got %d", len(allUsers.Users))
				return
			}

			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}

func TestInsertManyLargeBatch(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql insert many large batch",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MigrationUP(tt.args.ctx, tt.args.config); err != nil {
				t.Errorf("MigrationUP() failed: %v", err)
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}()

			conn, err := sqlfactory.NewDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test case: Insert a larger batch of users (10 users)
			largeBatchUsers := make([]sql.Record, 10)
			for i := 0; i < 10; i++ {
				largeBatchUsers[i] = &records.User{
					Name:         fmt.Sprintf("User%d", i+1),
					Email:        fmt.Sprintf("user%d@example.com", i+1),
					PasswordHash: fmt.Sprintf("hash%d", i+1),
					IsActive:     i % 2, // Alternate between 0 and 1
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				}
			}

			rowsAffected, err := conn.InsertMany(tt.args.ctx, largeBatchUsers)
			if err != nil {
				t.Errorf("InsertMany() with large batch failed: %v", err)
				return
			}

			if rowsAffected != 10 {
				t.Errorf("InsertMany() with large batch failed: expected 10 rows affected, got %d", rowsAffected)
				return
			}

			// Verify all records were inserted
			allUsers := &records.Users{}
			getFilter := &sql.Filter{
				Sort: sql.NewSort().Add("id", sql.Asc),
			}
			if err := conn.Get(tt.args.ctx, getFilter, []any{}, allUsers); err != nil {
				t.Errorf("Get() failed: %v", err)
				return
			}

			if len(allUsers.Users) != 10 {
				t.Errorf("Get() failed: expected 10 users, got %d", len(allUsers.Users))
				return
			}

			// Verify a few specific records
			if allUsers.Users[0].Name != "User1" {
				t.Errorf("Get() failed: expected name 'User1', got '%s'", allUsers.Users[0].Name)
			}
			if allUsers.Users[5].Name != "User6" {
				t.Errorf("Get() failed: expected name 'User6', got '%s'", allUsers.Users[5].Name)
			}
			if allUsers.Users[9].Name != "User10" {
				t.Errorf("Get() failed: expected name 'User10', got '%s'", allUsers.Users[9].Name)
			}

			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}

func TestInsertManyWithOptions(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql insert many with options",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MigrationUP(tt.args.ctx, tt.args.config); err != nil {
				t.Errorf("MigrationUP() failed: %v", err)
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}()

			conn, err := sqlfactory.NewDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Test case: Insert with options (though InsertMany doesn't use prepared statements)
			users := []sql.Record{
				&records.User{
					Name:         "Option User 1",
					Email:        "option1@example.com",
					PasswordHash: "option_hash1",
					IsActive:     1,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				},
				&records.User{
					Name:         "Option User 2",
					Email:        "option2@example.com",
					PasswordHash: "option_hash2",
					IsActive:     0,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				},
			}

			options := sql.Options{
				UsePrimaryDB: true,
				PreparedName: "test_insert_many",
			}

			rowsAffected, err := conn.InsertMany(tt.args.ctx, users, options)
			if err != nil {
				t.Errorf("InsertMany() with options failed: %v", err)
				return
			}

			if rowsAffected != 2 {
				t.Errorf("InsertMany() with options failed: expected 2 rows affected, got %d", rowsAffected)
				return
			}

			// Verify the inserted data
			allUsers := &records.Users{}
			getFilter := &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewValue("Option User%"),
					Operator: sql.LIKE,
				},
				Sort: sql.NewSort().Add("id", sql.Asc),
			}
			if err := conn.Get(tt.args.ctx, getFilter, []any{}, allUsers); err != nil {
				t.Errorf("Get() failed: %v", err)
				return
			}

			if len(allUsers.Users) != 2 {
				t.Errorf("Get() failed: expected 2 users, got %d", len(allUsers.Users))
				return
			}

			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}
