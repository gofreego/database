package tests

import (
	"context"
	"fmt"
	"strings"
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
	tests = []testCase{
		{
			name: "mysql",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
			},
		},
	}

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
func generateTestData(db sql.Database, data []sql.Record) error {
	noOfInserted, err := db.InsertMany(context.Background(), data, sql.Options{})
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	if noOfInserted != int64(len(data)) {
		return fmt.Errorf("expected %d records to be inserted, got %d", len(data), noOfInserted)
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
	if err := generateTestData(db, users); err != nil {
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Get users with names containing 'John'
			johnUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Operator: sql.LIKE,
					Value:    sql.NewValue("%John%"),
				},
			}, nil, johnUsers)

			if err != nil {
				t.Fatalf("failed to get users with 'John' in name: %v", err)
			}

			// check count of johnUsers
			if len(johnUsers.Users) != 2 {
				t.Errorf("expected 2 user, got %d", len(johnUsers.Users))
			}

			for _, user := range johnUsers.Users {
				if !strings.Contains(user.Name, "John") {
					t.Errorf("expected name containing 'John', got: %s", user.Name)
				}
			}

			// check with indexed value
			johnUsers = &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Operator: sql.LIKE,
					Value:    sql.NewIndexedValue(0),
				},
			}, []any{"%John%"}, johnUsers)

			if err != nil {
				t.Fatalf("failed to get users with 'John' in name: %v", err)
			}

			if len(johnUsers.Users) != 2 {
				t.Errorf("expected 2 user, got %d", len(johnUsers.Users))
			}

			for _, user := range johnUsers.Users {
				if !strings.Contains(user.Name, "John") {
					t.Errorf("expected name containing 'John', got: %s", user.Name)
				}
			}
			// not like
			notJohnUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Operator: sql.LIKE,
							Value:    sql.NewValue("%John%"),
						},
					},
					Operator: sql.NOT,
				},
			}, nil, notJohnUsers)

			if err != nil {
				t.Fatalf("failed to get users with not like 'John' in name: %v", err)
			}

			if len(notJohnUsers.Users) != 18 {
				t.Errorf("expected 18 user, got %d", len(notJohnUsers.Users))
			}

			for _, user := range notJohnUsers.Users {
				if strings.Contains(user.Name, "John") {
					t.Errorf("expected user with name not containing 'John', got: %s", user.Name)
				}
			}
		})
	}
}

// TestGetByConditionIN tests IN conditions
func TestGetByConditionIN(t *testing.T) {
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
				t.Fatalf("failed to get users with specific names: %v", err)
			}

			if len(specificUsers.Users) != 3 {
				t.Errorf("expected 3 users, got %d", len(specificUsers.Users))
			}

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

			// Test 2: Get users with specific active status
			activeStatusUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Operator: sql.IN,
					Value:    sql.NewValue([]any{0, 1}),
				},
			}, nil, activeStatusUsers)

			if err != nil {
				t.Fatalf("failed to get users with specific active status: %v", err)
			}

			if len(activeStatusUsers.Users) != 20 {
				t.Errorf("expected 20 users (all users), got %d", len(activeStatusUsers.Users))
			}

			// Test 4: NOT IN operator
			notInUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Operator: sql.IN,
							Value:    sql.NewValue([]any{"John Doe", "Jane Smith"}),
						},
					},
					Operator: sql.NOT,
				},
			}, nil, notInUsers)

			if err != nil {
				t.Fatalf("failed to get users NOT in specific names: %v", err)
			}

			if len(notInUsers.Users) != 18 {
				t.Errorf("expected 18 users (excluding John Doe and Jane Smith), got %d", len(notInUsers.Users))
			}

			for _, user := range notInUsers.Users {
				if user.Name == "John Doe" || user.Name == "Jane Smith" {
					t.Errorf("found excluded user in results: %s", user.Name)
				}
			}

		})
	}
}

// TestGetByConditionBETWEEN tests BETWEEN conditions
func TestGetByConditionBETWEEN(t *testing.T) {
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
					Value:    sql.NewValue([]any{int64(1640995200000), int64(1641513600000)}), // Jan 1-7, 2022
				},
			}, nil, earlyUsers)

			if err != nil {
				t.Fatalf("failed to get early users: %v", err)
			}

			// Should have users created in first week
			if len(earlyUsers.Users) < 1 {
				t.Errorf("expected at least 1 early user, got %d", len(earlyUsers.Users))
			}

			for _, user := range earlyUsers.Users {
				if user.CreatedAt < 1640995200000 || user.CreatedAt > 1641513600000 {
					t.Errorf("user created_at %d is outside expected range", user.CreatedAt)
				}
			}

			// Test 2: Get users with IDs between 5 and 15
			midUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "id",
					Operator: sql.BETWEEN,
					Value:    sql.NewValue([]any{int64(5), int64(15)}),
				},
			}, nil, midUsers)

			if err != nil {
				t.Fatalf("failed to get mid-range users: %v", err)
			}

			// Should have users with IDs 5-15
			if len(midUsers.Users) < 1 {
				t.Errorf("expected at least 1 mid-range user, got %d", len(midUsers.Users))
			}

			for _, user := range midUsers.Users {
				if user.Id < 5 || user.Id > 15 {
					t.Errorf("user ID %d is outside expected range 5-15", user.Id)
				}
			}

			// Test 3: NOT BETWEEN operator
			notBetweenUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "id",
							Operator: sql.BETWEEN,
							Value:    sql.NewValue([]any{int64(5), int64(15)}),
						},
					},
					Operator: sql.NOT,
				},
			}, nil, notBetweenUsers)

			if err != nil {
				t.Fatalf("failed to get users NOT between IDs 5-15: %v", err)
			}

			for _, user := range notBetweenUsers.Users {
				if user.Id >= 5 && user.Id <= 15 {
					t.Errorf("found user with ID %d in excluded range 5-15", user.Id)
				}
			}
		})
	}
}

// TestGetWithSorting tests sorting functionality
func TestGetWithSorting(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Sort by name ascending
			sortedByName := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().Add("name", sql.Asc),
			}, nil, sortedByName)

			if err != nil {
				t.Fatalf("failed to get users sorted by name: %v", err)
			}

			if len(sortedByName.Users) < 2 {
				t.Fatalf("need at least 2 users to test sorting")
			}

			// Verify ascending order
			for i := 1; i < len(sortedByName.Users); i++ {
				if sortedByName.Users[i-1].Name > sortedByName.Users[i].Name {
					t.Errorf("names not in ascending order: %s > %s",
						sortedByName.Users[i-1].Name, sortedByName.Users[i].Name)
				}
			}

			// Test 2: Sort by created_at descending
			sortedByDate := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().Add("created_at", sql.Desc),
			}, nil, sortedByDate)

			if err != nil {
				t.Fatalf("failed to get users sorted by created_at: %v", err)
			}

			if len(sortedByDate.Users) < 2 {
				t.Fatalf("need at least 2 users to test sorting")
			}

			// Verify descending order
			for i := 1; i < len(sortedByDate.Users); i++ {
				if sortedByDate.Users[i-1].CreatedAt < sortedByDate.Users[i].CreatedAt {
					t.Errorf("created_at not in descending order: %d < %d",
						sortedByDate.Users[i-1].CreatedAt, sortedByDate.Users[i].CreatedAt)
				}
			}

			// Test 3: Multi-field sorting (is_active DESC, name ASC)
			multiSorted := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort: sql.NewSort().
					Add("is_active", sql.Desc).
					Add("name", sql.Asc),
			}, nil, multiSorted)

			if err != nil {
				t.Fatalf("failed to get users with multi-field sorting: %v", err)
			}

			if len(multiSorted.Users) < 2 {
				t.Fatalf("need at least 2 users to test multi-field sorting")
			}

			// Verify multi-field sorting logic
			for i := 1; i < len(multiSorted.Users); i++ {
				prev := multiSorted.Users[i-1]
				curr := multiSorted.Users[i]

				// If is_active is different, it should be in descending order
				if prev.IsActive != curr.IsActive {
					if prev.IsActive < curr.IsActive {
						t.Errorf("is_active not in descending order: %d < %d", prev.IsActive, curr.IsActive)
					}
				} else {
					// If is_active is same, name should be in ascending order
					if prev.Name > curr.Name {
						t.Errorf("names not in ascending order when is_active is same: %s > %s",
							prev.Name, curr.Name)
					}
				}
			}
		})
	}
}

// TestGetWithPagination tests pagination functionality
func TestGetWithPagination(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Limit results
			limitedUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Limit: sql.NewValue(int64(5)),
			}, nil, limitedUsers)

			if err != nil {
				t.Fatalf("failed to get limited users: %v", err)
			}

			if len(limitedUsers.Users) != 5 {
				t.Errorf("expected 5 users with limit, got %d", len(limitedUsers.Users))
			}

			// Test 2: Offset results (only test with databases that support it)
			// MySQL requires LIMIT with OFFSET, so we'll test that combination
			offsetUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Limit:  sql.NewValue(int64(10)),
				Offset: sql.NewValue(int64(10)),
			}, nil, offsetUsers)

			if err != nil {
				t.Fatalf("failed to get offset users: %v", err)
			}

			// Should have users after offset (up to limit)
			if len(offsetUsers.Users) > 10 {
				t.Errorf("expected at most 10 users with limit and offset, got %d", len(offsetUsers.Users))
			}

			// Test 3: Limit and offset together
			paginatedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Limit:  sql.NewValue(int64(3)),
				Offset: sql.NewValue(int64(5)),
			}, nil, paginatedUsers)

			if err != nil {
				t.Fatalf("failed to get paginated users: %v", err)
			}

			if len(paginatedUsers.Users) != 3 {
				t.Errorf("expected 3 users with limit and offset, got %d", len(paginatedUsers.Users))
			}

			// Test 4: Pagination with sorting
			sortedPaginatedUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Sort:   sql.NewSort().Add("name", sql.Asc),
				Limit:  sql.NewValue(int64(4)),
				Offset: sql.NewValue(int64(8)),
			}, nil, sortedPaginatedUsers)

			if err != nil {
				t.Fatalf("failed to get sorted paginated users: %v", err)
			}

			if len(sortedPaginatedUsers.Users) != 4 {
				t.Errorf("expected 4 users with sorted pagination, got %d", len(sortedPaginatedUsers.Users))
			}

			// Verify the results are sorted
			for i := 1; i < len(sortedPaginatedUsers.Users); i++ {
				if sortedPaginatedUsers.Users[i-1].Name > sortedPaginatedUsers.Users[i].Name {
					t.Errorf("names not in ascending order: %s > %s",
						sortedPaginatedUsers.Users[i-1].Name, sortedPaginatedUsers.Users[i].Name)
				}
			}
		})
	}
}

// TestGetWithComplexConditions tests complex query combinations
func TestGetWithComplexConditions(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Active users with bcrypt password hashes
			activeBcryptUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "is_active",
							Operator: sql.EQ,
							Value:    sql.NewValue(1),
						},
						{
							Field:    "password_hash",
							Operator: sql.LIKE,
							Value:    sql.NewValue("bcrypt_hash_%"),
						},
					},
					Operator: sql.AND,
				},
			}, nil, activeBcryptUsers)

			if err != nil {
				t.Fatalf("failed to get active bcrypt users: %v", err)
			}

			// Verify all returned users are active and have bcrypt hashes
			for _, user := range activeBcryptUsers.Users {
				if user.IsActive != 1 {
					t.Errorf("expected active user, got IsActive=%d", user.IsActive)
				}
				if !strings.HasPrefix(user.PasswordHash, "bcrypt_hash_") {
					t.Errorf("expected bcrypt hash, got: %s", user.PasswordHash)
				}
			}

			// Test 2: Users with specific names OR inactive status
			namedOrInactiveUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "name",
							Operator: sql.IN,
							Value:    sql.NewValue([]any{"John Doe", "Jane Smith"}),
						},
						{
							Field:    "is_active",
							Operator: sql.EQ,
							Value:    sql.NewValue(0),
						},
					},
					Operator: sql.OR,
				},
			}, nil, namedOrInactiveUsers)

			if err != nil {
				t.Fatalf("failed to get named or inactive users: %v", err)
			}

			// Verify all returned users match the OR condition
			for _, user := range namedOrInactiveUsers.Users {
				isNamed := user.Name == "John Doe" || user.Name == "Jane Smith"
				isInactive := user.IsActive == 0
				if !isNamed && !isInactive {
					t.Errorf("user %s (active=%d) doesn't match OR condition", user.Name, user.IsActive)
				}
			}

			// Test 3: Complex nested conditions
			complexUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Conditions: []sql.Condition{
						{
							Field:    "is_active",
							Operator: sql.EQ,
							Value:    sql.NewValue(1),
						},
						{
							Conditions: []sql.Condition{
								{
									Field:    "name",
									Operator: sql.LIKE,
									Value:    sql.NewValue("%John%"),
								},
								{
									Field:    "email",
									Operator: sql.LIKE,
									Value:    sql.NewValue("%.com"),
								},
							},
							Operator: sql.AND,
						},
					},
					Operator: sql.AND,
				},
			}, nil, complexUsers)

			if err != nil {
				t.Fatalf("failed to get users with complex conditions: %v", err)
			}

			// Verify all returned users match the complex condition
			for _, user := range complexUsers.Users {
				if user.IsActive != 1 {
					t.Errorf("expected active user, got IsActive=%d", user.IsActive)
				}
				if !strings.Contains(user.Name, "John") {
					t.Errorf("expected name containing 'John', got: %s", user.Name)
				}
				if !strings.HasSuffix(user.Email, ".com") {
					t.Errorf("expected email ending with .com, got: %s", user.Email)
				}
			}
		})
	}
}

// TestGetEdgeCases tests edge cases and boundary conditions
func TestGetEdgeCases(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupTestDatabase(t, tt.args.config)
			defer cleanup()

			ctx := tt.args.ctx

			// Test 1: Get non-existent user
			nonExistentUsers := &records.Users{}
			err := db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "email",
					Operator: sql.EQ,
					Value:    sql.NewValue("nonexistent@example.com"),
				},
			}, nil, nonExistentUsers)

			if err != nil {
				t.Fatalf("failed to get non-existent user: %v", err)
			}

			if len(nonExistentUsers.Users) != 0 {
				t.Errorf("expected 0 users for non-existent email, got %d", len(nonExistentUsers.Users))
			}

			// Test 2: Get all users without any filter
			allUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{}, nil, allUsers)

			if err != nil {
				t.Fatalf("failed to get all users: %v", err)
			}

			if len(allUsers.Users) != 20 {
				t.Errorf("expected 20 users total, got %d", len(allUsers.Users))
			}

			// Test 6: Get users with exact timestamp match
			exactTimeUsers := &records.Users{}
			err = db.Get(ctx, &sql.Filter{
				Condition: &sql.Condition{
					Field:    "created_at",
					Operator: sql.EQ,
					Value:    sql.NewValue(int64(1640995200000)), // Jan 1, 2022 00:00:00
				},
			}, nil, exactTimeUsers)

			if err != nil {
				t.Fatalf("failed to get users with exact timestamp: %v", err)
			}

			// Should have at least one user created at that exact time
			if len(exactTimeUsers.Users) < 1 {
				t.Errorf("expected at least 1 user with exact timestamp, got %d", len(exactTimeUsers.Users))
			}

			for _, user := range exactTimeUsers.Users {
				if user.CreatedAt != 1640995200000 {
					t.Errorf("expected exact timestamp 1640995200000, got %d", user.CreatedAt)
				}
			}
		})
	}
}
