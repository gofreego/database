package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

/*
This file contains comprehensive tests for the get functionality of the sql package.
This covers all the different ways that a get query can be constructed and executed.
*/

var (
	users []sql.Record = []sql.Record{
		&records.User{
			Name:         "John Doe",
			Email:        "john.doe@example.com",
			PasswordHash: "bcrypt_hash_123",
			IsActive:     1,
			CreatedAt:    1640995200000, // 2022-01-01 00:00:00
			UpdatedAt:    1640995200000,
		},
		&records.User{
			Name:         "Jane Smith",
			Email:        "jane.smith@example.com",
			PasswordHash: "sha256_hash_456",
			IsActive:     1,
			CreatedAt:    1641081600000, // 2022-01-02 00:00:00
			UpdatedAt:    1641168000000, // 2022-01-03 00:00:00
		},
		&records.User{
			Name:         "Michael Johnson",
			Email:        "michael.johnson@example.com",
			PasswordHash: "bcrypt_hash_789",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641168000000, // 2022-01-03 00:00:00
			UpdatedAt:    1641254400000, // 2022-01-04 00:00:00
		},
		&records.User{
			Name:         "Emily Davis",
			Email:        "emily.davis@example.com",
			PasswordHash: "sha256_hash_abc",
			IsActive:     1,
			CreatedAt:    1641254400000, // 2022-01-04 00:00:00
			UpdatedAt:    1641254400000,
		},
		&records.User{
			Name:         "David Wilson",
			Email:        "david.wilson@example.com",
			PasswordHash: "bcrypt_hash_def",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641340800000, // 2022-01-05 00:00:00
			UpdatedAt:    1641427200000, // 2022-01-06 00:00:00
		},
		&records.User{
			Name:         "Sarah Brown",
			Email:        "sarah.brown@example.com",
			PasswordHash: "sha256_hash_ghi",
			IsActive:     1,
			CreatedAt:    1641427200000, // 2022-01-06 00:00:00
			UpdatedAt:    1641427200000,
		},
		&records.User{
			Name:         "Robert Taylor",
			Email:        "robert.taylor@example.com",
			PasswordHash: "bcrypt_hash_jkl",
			IsActive:     1,
			CreatedAt:    1641513600000, // 2022-01-07 00:00:00
			UpdatedAt:    1641600000000, // 2022-01-08 00:00:00
		},
		&records.User{
			Name:         "Lisa Anderson",
			Email:        "lisa.anderson@example.com",
			PasswordHash: "sha256_hash_mno",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641600000000, // 2022-01-08 00:00:00
			UpdatedAt:    1641686400000, // 2022-01-09 00:00:00
		},
		&records.User{
			Name:         "James Martinez",
			Email:        "james.martinez@example.com",
			PasswordHash: "bcrypt_hash_pqr",
			IsActive:     1,
			CreatedAt:    1641686400000, // 2022-01-09 00:00:00
			UpdatedAt:    1641686400000,
		},
		&records.User{
			Name:         "Jennifer Garcia",
			Email:        "jennifer.garcia@example.com",
			PasswordHash: "sha256_hash_stu",
			IsActive:     1,
			CreatedAt:    1641772800000, // 2022-01-10 00:00:00
			UpdatedAt:    1641859200000, // 2022-01-11 00:00:00
		},
		&records.User{
			Name:         "Christopher Rodriguez",
			Email:        "christopher.rodriguez@example.com",
			PasswordHash: "bcrypt_hash_vwx",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641859200000, // 2022-01-11 00:00:00
			UpdatedAt:    1641945600000, // 2022-01-12 00:00:00
		},
		&records.User{
			Name:         "Amanda Lewis",
			Email:        "amanda.lewis@example.com",
			PasswordHash: "sha256_hash_yz1",
			IsActive:     1,
			CreatedAt:    1641945600000, // 2022-01-12 00:00:00
			UpdatedAt:    1641945600000,
		},
		&records.User{
			Name:         "Daniel Lee",
			Email:        "daniel.lee@example.com",
			PasswordHash: "bcrypt_hash_234",
			IsActive:     1,
			CreatedAt:    1642032000000, // 2022-01-13 00:00:00
			UpdatedAt:    1642118400000, // 2022-01-14 00:00:00
		},
		&records.User{
			Name:         "Michelle White",
			Email:        "michelle.white@example.com",
			PasswordHash: "sha256_hash_567",
			IsActive:     0,             // Inactive user
			CreatedAt:    1642118400000, // 2022-01-14 00:00:00
			UpdatedAt:    1642204800000, // 2022-01-15 00:00:00
		},
		&records.User{
			Name:         "Kevin Harris",
			Email:        "kevin.harris@example.com",
			PasswordHash: "bcrypt_hash_890",
			IsActive:     1,
			CreatedAt:    1642204800000, // 2022-01-15 00:00:00
			UpdatedAt:    1642204800000,
		},
		&records.User{
			Name:         "Nicole Clark",
			Email:        "nicole.clark@example.com",
			PasswordHash: "sha256_hash_abc",
			IsActive:     1,
			CreatedAt:    1642291200000, // 2022-01-16 00:00:00
			UpdatedAt:    1642377600000, // 2022-01-17 00:00:00
		},
		&records.User{
			Name:         "Steven Hall",
			Email:        "steven.hall@example.com",
			PasswordHash: "bcrypt_hash_def",
			IsActive:     0,             // Inactive user
			CreatedAt:    1642377600000, // 2022-01-17 00:00:00
			UpdatedAt:    1642464000000, // 2022-01-18 00:00:00
		},
		&records.User{
			Name:         "Rachel Young",
			Email:        "rachel.young@example.com",
			PasswordHash: "sha256_hash_ghi",
			IsActive:     1,
			CreatedAt:    1642464000000, // 2022-01-18 00:00:00
			UpdatedAt:    1642464000000,
		},
		&records.User{
			Name:         "Thomas King",
			Email:        "thomas.king@example.com",
			PasswordHash: "bcrypt_hash_jkl",
			IsActive:     1,
			CreatedAt:    1642550400000, // 2022-01-19 00:00:00
			UpdatedAt:    1642636800000, // 2022-01-20 00:00:00
		},
		&records.User{
			Name:         "Jessica Wright",
			Email:        "jessica.wright@example.com",
			PasswordHash: "sha256_hash_mno",
			IsActive:     0,             // Inactive user
			CreatedAt:    1642636800000, // 2022-01-20 00:00:00
			UpdatedAt:    1642723200000, // 2022-01-21 00:00:00
		},
	}
)

// generateTestData inserts the test users into the database
func generateTestData(db sql.Database) error {
	noOfInserted, err := db.InsertMany(context.Background(), users, sql.Options{})
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	if noOfInserted != int64(len(users)) {
		return fmt.Errorf("expected %d records to be inserted, got %d", len(users), noOfInserted)
	}
	return nil
}

// setupTestDatabase sets up the test environment
func setupTestDatabase(t *testing.T, config *sqlfactory.Config) (sql.Database, func()) {
	ctx := context.Background()

	// Run migrations
	MigrationUP(ctx, config, t)

	// Create database connection
	db, err := sqlfactory.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("failed to create database connection: %v", err)
	}

	// Test connection
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	// Insert test data
	if err := generateTestData(db); err != nil {
		t.Fatalf("failed to generate test data: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		db.Close(ctx)
		MigrationDown(ctx, config, t)
	}

	return db, cleanup
}

// TestGetByConditionEQ tests equality conditions
func TestGetByConditionEQ(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql equality filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql equality filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql equality filter",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Get active users with direct value
			activeUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.EQ,
					Value:    sql.NewValue(1),
				},
			}, nil, activeUsers)

			if err != nil {
				t.Fatalf("failed to get active users: %v", err)
			}

			// Should have 13 active users
			if len(activeUsers.Users) != 13 {
				t.Errorf("expected 13 active users, got %d", len(activeUsers.Users))
			}

			// Verify all returned users are active
			for _, user := range activeUsers.Users {
				if user.IsActive != 1 {
					t.Errorf("expected active user, got IsActive=%d", user.IsActive)
				}
			}

			// Test 2: Get active users with parameterized value
			activeUsers = &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.EQ,
					Value:    &sql.Value{Index: 0},
				},
			}, []any{1}, activeUsers)

			if err != nil {
				t.Fatalf("failed to get active users with parameterized value: %v", err)
			}

			if len(activeUsers.Users) != 13 {
				t.Errorf("expected 13 active users with parameterized value, got %d", len(activeUsers.Users))
			}

			// Test 3: Get specific user by email
			specificUser := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "email",
					Operator: sql.EQ,
					Value:    sql.NewValue("john.doe@example.com"),
				},
			}, nil, specificUser)

			if err != nil {
				t.Fatalf("failed to get specific user: %v", err)
			}

			if len(specificUser.Users) != 1 {
				t.Errorf("expected 1 user, got %d", len(specificUser.Users))
			}

			if specificUser.Users[0].Name != "John Doe" {
				t.Errorf("expected user 'John Doe', got '%s'", specificUser.Users[0].Name)
			}
		})
	}
}

// TestGetByConditionNEQ tests not-equal conditions
func TestGetByConditionNEQ(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql not-equal filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql not-equal filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql not-equal filter",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Get inactive users (not equal to 1)
			inactiveUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.NEQ,
					Value:    sql.NewValue(1),
				},
			}, nil, inactiveUsers)

			if err != nil {
				t.Fatalf("failed to get inactive users: %v", err)
			}

			// Should have 7 inactive users
			if len(inactiveUsers.Users) != 7 {
				t.Errorf("expected 7 inactive users, got %d", len(inactiveUsers.Users))
			}

			// Verify all returned users are inactive
			for _, user := range inactiveUsers.Users {
				if user.IsActive != 0 {
					t.Errorf("expected inactive user, got IsActive=%d", user.IsActive)
				}
			}

			// Test 2: Get users not with specific email
			otherUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "email",
					Operator: sql.NEQ,
					Value:    sql.NewValue("john.doe@example.com"),
				},
			}, nil, otherUsers)

			if err != nil {
				t.Fatalf("failed to get other users: %v", err)
			}

			// Should have 19 users (excluding john.doe@example.com)
			if len(otherUsers.Users) != 19 {
				t.Errorf("expected 19 other users, got %d", len(otherUsers.Users))
			}

			// Verify john.doe@example.com is not in results
			for _, user := range otherUsers.Users {
				if user.Email == "john.doe@example.com" {
					t.Errorf("found excluded user in results: %s", user.Email)
				}
			}
		})
	}
}
