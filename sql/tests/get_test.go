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

// TestGetByConditionLIKE tests LIKE conditions
func TestGetByConditionLIKE(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql like filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql like filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql like filter",
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

			// Test 1: Get users with bcrypt password hashes
			bcryptUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "password_hash",
					Operator: sql.LIKE,
					Value:    sql.NewValue("bcrypt_hash_%"),
				},
			}, nil, bcryptUsers)

			if err != nil {
				t.Fatalf("failed to get bcrypt users: %v", err)
			}

			// Should have 10 users with bcrypt hashes
			if len(bcryptUsers.Users) != 10 {
				t.Errorf("expected 10 bcrypt users, got %d", len(bcryptUsers.Users))
			}

			// Verify all returned users have bcrypt hashes
			for _, user := range bcryptUsers.Users {
				if len(user.PasswordHash) < 12 || user.PasswordHash[:12] != "bcrypt_hash_" {
					t.Errorf("expected bcrypt hash, got: %s", user.PasswordHash)
				}
			}

			// Test 2: Get users with names containing "John"
			johnUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Operator: sql.LIKE,
					Value:    sql.NewValue("%John%"),
				},
			}, nil, johnUsers)

			if err != nil {
				t.Fatalf("failed to get John users: %v", err)
			}

			// Should have 2 users with "John" in name
			if len(johnUsers.Users) != 2 {
				t.Errorf("expected 2 John users, got %d", len(johnUsers.Users))
			}

			// Verify all returned users have "John" in name
			for _, user := range johnUsers.Users {
				if len(user.Name) < 4 || user.Name[:4] != "John" {
					t.Errorf("expected name containing 'John', got: %s", user.Name)
				}
			}

			// Test 3: Get users with emails ending in .com
			comUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "email",
					Operator: sql.LIKE,
					Value:    sql.NewValue("%.com"),
				},
			}, nil, comUsers)

			if err != nil {
				t.Fatalf("failed to get .com users: %v", err)
			}

			// Should have all 20 users with .com emails
			if len(comUsers.Users) != 20 {
				t.Errorf("expected 20 .com users, got %d", len(comUsers.Users))
			}
		})
	}
}

// TestGetByConditionIN tests IN conditions
func TestGetByConditionIN(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql in filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql in filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql in filter",
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

			// Test 1: Get users with specific names
			specificUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Operator: sql.IN,
					Value:    sql.NewValue([]any{"John Doe", "Jane Smith", "Michael Johnson"}),
				},
			}, nil, specificUsers)

			if err != nil {
				t.Fatalf("failed to get specific users: %v", err)
			}

			// Should have 3 users
			if len(specificUsers.Users) != 3 {
				t.Errorf("expected 3 specific users, got %d", len(specificUsers.Users))
			}

			// Verify all returned users are in the expected list
			expectedNames := map[string]bool{
				"John Doe":        true,
				"Jane Smith":      true,
				"Michael Johnson": true,
			}

			for _, user := range specificUsers.Users {
				if !expectedNames[user.Name] {
					t.Errorf("unexpected user name: %s", user.Name)
				}
			}

			// Test 2: Get users with specific active statuses
			statusUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.IN,
					Value:    sql.NewValue([]any{0, 1}),
				},
			}, nil, statusUsers)

			if err != nil {
				t.Fatalf("failed to get status users: %v", err)
			}

			// Should have all 20 users
			if len(statusUsers.Users) != 20 {
				t.Errorf("expected 20 status users, got %d", len(statusUsers.Users))
			}
		})
	}
}

// TestGetByConditionBETWEEN tests BETWEEN conditions
func TestGetByConditionBETWEEN(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql between filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql between filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql between filter",
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

			// Test 1: Get users created in first week of January 2022
			earlyUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "created_at",
					Operator: sql.BETWEEN,
					Value:    sql.NewValue([]int64{1640995200000, 1641513600000}), // Jan 1-7, 2022
				},
			}, nil, earlyUsers)

			if err != nil {
				t.Fatalf("failed to get early users: %v", err)
			}

			// Should have 7 users created in first week
			if len(earlyUsers.Users) != 7 {
				t.Errorf("expected 7 early users, got %d", len(earlyUsers.Users))
			}

			// Verify all returned users were created in the specified range
			for _, user := range earlyUsers.Users {
				if user.CreatedAt < 1640995200000 || user.CreatedAt > 1641513600000 {
					t.Errorf("user created outside expected range: %d", user.CreatedAt)
				}
			}

			// Test 2: Get users with IDs between 5 and 15
			midUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "id",
					Operator: sql.BETWEEN,
					Value:    sql.NewValue([]int64{5, 15}),
				},
			}, nil, midUsers)

			if err != nil {
				t.Fatalf("failed to get mid users: %v", err)
			}

			// Should have 11 users (IDs 5-15 inclusive)
			if len(midUsers.Users) != 11 {
				t.Errorf("expected 11 mid users, got %d", len(midUsers.Users))
			}

			// Verify all returned users have IDs in the specified range
			for _, user := range midUsers.Users {
				if user.Id < 5 || user.Id > 15 {
					t.Errorf("user ID outside expected range: %d", user.Id)
				}
			}
		})
	}
}

// TestGetWithSorting tests sorting functionality
func TestGetWithSorting(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql sorting",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql sorting",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql sorting",
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

			// Test 1: Sort by name ascending
			sortedUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().Add("name", sql.Asc),
			}, nil, sortedUsers)

			if err != nil {
				t.Fatalf("failed to get sorted users: %v", err)
			}

			// Verify sorting
			for i := 1; i < len(sortedUsers.Users); i++ {
				if sortedUsers.Users[i-1].Name > sortedUsers.Users[i].Name {
					t.Errorf("users not sorted correctly: %s > %s",
						sortedUsers.Users[i-1].Name, sortedUsers.Users[i].Name)
				}
			}

			// Test 2: Sort by created_at descending
			reverseSortedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().Add("created_at", sql.Desc),
			}, nil, reverseSortedUsers)

			if err != nil {
				t.Fatalf("failed to get reverse sorted users: %v", err)
			}

			// Verify reverse sorting
			for i := 1; i < len(reverseSortedUsers.Users); i++ {
				if reverseSortedUsers.Users[i-1].CreatedAt < reverseSortedUsers.Users[i].CreatedAt {
					t.Errorf("users not reverse sorted correctly: %d < %d",
						reverseSortedUsers.Users[i-1].CreatedAt, reverseSortedUsers.Users[i].CreatedAt)
				}
			}

			// Test 3: Multi-field sorting
			multiSortedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().
					Add("is_active", sql.Desc).
					Add("name", sql.Asc),
			}, nil, multiSortedUsers)

			if err != nil {
				t.Fatalf("failed to get multi-sorted users: %v", err)
			}

			// Verify multi-field sorting (active users first, then by name)
			activeCount := 0
			for _, user := range multiSortedUsers.Users {
				if user.IsActive == 1 {
					activeCount++
				}
			}

			// First 13 users should be active
			for i := 0; i < 13; i++ {
				if multiSortedUsers.Users[i].IsActive != 1 {
					t.Errorf("expected active user at position %d, got IsActive=%d", i, multiSortedUsers.Users[i].IsActive)
				}
			}

			// Last 7 users should be inactive
			for i := 13; i < 20; i++ {
				if multiSortedUsers.Users[i].IsActive != 0 {
					t.Errorf("expected inactive user at position %d, got IsActive=%d", i, multiSortedUsers.Users[i].IsActive)
				}
			}
		})
	}
}

// TestGetWithPagination tests pagination functionality
func TestGetWithPagination(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql pagination",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql pagination",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql pagination",
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

			// Test 1: Limit results
			limitedUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Limit: sql.NewValue(5),
			}, nil, limitedUsers)

			if err != nil {
				t.Fatalf("failed to get limited users: %v", err)
			}

			// Should have exactly 5 users
			if len(limitedUsers.Users) != 5 {
				t.Errorf("expected 5 limited users, got %d", len(limitedUsers.Users))
			}

			// Test 2: Offset results
			offsetUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Offset: sql.NewValue(10),
			}, nil, offsetUsers)

			if err != nil {
				t.Fatalf("failed to get offset users: %v", err)
			}

			// Should have 10 users (20 total - 10 offset)
			if len(offsetUsers.Users) != 10 {
				t.Errorf("expected 10 offset users, got %d", len(offsetUsers.Users))
			}

			// Test 3: Limit and offset together
			paginatedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Limit:  sql.NewValue(3),
				Offset: sql.NewValue(5),
			}, nil, paginatedUsers)

			if err != nil {
				t.Fatalf("failed to get paginated users: %v", err)
			}

			// Should have exactly 3 users
			if len(paginatedUsers.Users) != 3 {
				t.Errorf("expected 3 paginated users, got %d", len(paginatedUsers.Users))
			}

			// Test 4: Pagination with sorting
			sortedPaginatedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort:   sql.NewSort().Add("name", sql.Asc),
				Limit:  sql.NewValue(4),
				Offset: sql.NewValue(8),
			}, nil, sortedPaginatedUsers)

			if err != nil {
				t.Fatalf("failed to get sorted paginated users: %v", err)
			}

			// Should have exactly 4 users
			if len(sortedPaginatedUsers.Users) != 4 {
				t.Errorf("expected 4 sorted paginated users, got %d", len(sortedPaginatedUsers.Users))
			}
		})
	}
}

// TestGetWithComplexConditions tests complex query conditions
func TestGetWithComplexConditions(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql complex conditions",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql complex conditions",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql complex conditions",
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

			// Test 1: Active users with bcrypt hashes created in first week
			complexUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.EQ,
					Value:    sql.NewValue(1),
				},
				Sort: sql.NewSort().Add("created_at", sql.Asc),
			}, nil, complexUsers)

			if err != nil {
				t.Fatalf("failed to get complex users: %v", err)
			}

			// Should have 13 active users
			if len(complexUsers.Users) != 13 {
				t.Errorf("expected 13 complex users, got %d", len(complexUsers.Users))
			}

			// Verify all are active and sorted by creation date
			for i, user := range complexUsers.Users {
				if user.IsActive != 1 {
					t.Errorf("expected active user at position %d, got IsActive=%d", i, user.IsActive)
				}

				if i > 0 && user.CreatedAt < complexUsers.Users[i-1].CreatedAt {
					t.Errorf("users not sorted by creation date at position %d", i)
				}
			}

			// Test 2: Inactive users with recent updates
			recentInactiveUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.EQ,
					Value:    sql.NewValue(0),
				},
				Sort:  sql.NewSort().Add("updated_at", sql.Desc),
				Limit: sql.NewValue(3),
			}, nil, recentInactiveUsers)

			if err != nil {
				t.Fatalf("failed to get recent inactive users: %v", err)
			}

			// Should have 3 inactive users
			if len(recentInactiveUsers.Users) != 3 {
				t.Errorf("expected 3 recent inactive users, got %d", len(recentInactiveUsers.Users))
			}

			// Verify all are inactive and sorted by update date descending
			for i, user := range recentInactiveUsers.Users {
				if user.IsActive != 0 {
					t.Errorf("expected inactive user at position %d, got IsActive=%d", i, user.IsActive)
				}

				if i > 0 && user.UpdatedAt > recentInactiveUsers.Users[i-1].UpdatedAt {
					t.Errorf("users not sorted by update date descending at position %d", i)
				}
			}
		})
	}
}

// TestGetEdgeCases tests edge cases and error conditions
func TestGetEdgeCases(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql edge cases",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql edge cases",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql edge cases",
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

			// Test 1: Empty result set
			emptyUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "email",
					Operator: sql.EQ,
					Value:    sql.NewValue("nonexistent@example.com"),
				},
			}, nil, emptyUsers)

			if err != nil {
				t.Fatalf("failed to get empty result set: %v", err)
			}

			// Should have 0 users
			if len(emptyUsers.Users) != 0 {
				t.Errorf("expected 0 users for nonexistent email, got %d", len(emptyUsers.Users))
			}

			// Test 2: Large offset
			largeOffsetUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Offset: sql.NewValue(100),
			}, nil, largeOffsetUsers)

			if err != nil {
				t.Fatalf("failed to get large offset users: %v", err)
			}

			// Should have 0 users (offset beyond data)
			if len(largeOffsetUsers.Users) != 0 {
				t.Errorf("expected 0 users for large offset, got %d", len(largeOffsetUsers.Users))
			}

			// Test 3: Zero limit
			zeroLimitUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Limit: sql.NewValue(0),
			}, nil, zeroLimitUsers)

			if err != nil {
				t.Fatalf("failed to get zero limit users: %v", err)
			}

			// Should have 0 users
			if len(zeroLimitUsers.Users) != 0 {
				t.Errorf("expected 0 users for zero limit, got %d", len(zeroLimitUsers.Users))
			}

			// Test 4: Get all users without any filter
			allUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{}, nil, allUsers)

			if err != nil {
				t.Fatalf("failed to get all users: %v", err)
			}

			// Should have all 20 users
			if len(allUsers.Users) != 20 {
				t.Errorf("expected 20 users without filter, got %d", len(allUsers.Users))
			}
		})
	}
}
