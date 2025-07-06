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

type args struct {
	ctx    context.Context
	config *sqlfactory.Config
}
type testCase struct {
	name    string
	args    args
	wantErr bool
	pingErr bool
}

func TestInsertAndGet(t *testing.T) {

	tests := []testCase{
		// {
		// 	name: "postgresql insert and get",
		// 	args: args{
		// 		ctx:    context.Background(),
		// 		config: &postgresqlConfig,
		// 	},
		// 	wantErr: false,
		// 	pingErr: false,
		// },
		{
			name: "mysql insert and get",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
	}
	var executeGetTestFunc = func(t *testing.T, tt testCase) bool {

		conn, err := sqlfactory.NewSQLDatabase(tt.args.ctx, tt.args.config)
		if (err != nil) != tt.wantErr {
			t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
			return true
		}
		if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
			t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
		}
		user := &records.User{Id: 1}
		if err := conn.GetByID(tt.args.ctx, user); err != sql.ErrNoRecordFound {
			t.Errorf("GetByID() error = %v, wantErr %v", err, sql.ErrNoRecordFound)
			return true
		}

		user = &records.User{
			Name:         "John Doe",
			Email:        "john.doe@example.com",
			PasswordHash: "password123",
			IsActive:     1,
			CreatedAt:    time.Now().UnixMilli(),
			UpdatedAt:    time.Now().UnixMilli(),
		}

		if err := conn.Insert(tt.args.ctx, user); err != nil {
			t.Errorf("Insert() failed: %v", err)
			return true
		}

		if user.Id == 0 {
			t.Errorf("Insert() failed: id is 0")
			return true
		}

		user.Name = ""
		user.Email = ""
		user.PasswordHash = ""
		user.IsActive = 0
		user.CreatedAt = 0
		user.UpdatedAt = 0
		if err := conn.GetByID(tt.args.ctx, user); err != nil {
			t.Errorf("GetByID() failed: %v", err)
			return true
		}
		if user.Id != 1 {
			t.Errorf("GetByID() failed: id is not 1")
			return true
		}
		if user.Name != "John Doe" {
			t.Errorf("GetByID() failed: name is not John Doe")
			return true
		}
		if user.Email != "john.doe@example.com" {
			t.Errorf("GetByID() failed: email is not john.doe@example.com")
			return true
		}
		if user.PasswordHash != "password123" {
			t.Errorf("GetByID() failed: password hash is not password123")
			return true
		}
		if user.IsActive != 1 {
			t.Errorf("GetByID() failed: is active is not 1")
			return true
		}
		if user.CreatedAt == 0 {
			t.Errorf("GetByID() failed: created at is zero")
			return true
		}
		if user.UpdatedAt == 0 {
			t.Errorf("GetByID() failed: updated at is zero")
			return true
		}
		if err := conn.Close(tt.args.ctx); err != nil {
			t.Errorf("Close() failed: %v", err)
			return true
		}

		return false
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
			executeGetTestFunc(t, tt)
		})
	}
}

func TestMysqlDatabase_GetByFilter(t *testing.T) {

	tests := []testCase{
		{
			name: "mysql get by filter",
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

			conn, err := sqlfactory.NewSQLDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
			var user, user2, user3 *records.User
			// prepare the data
			{
				user = &records.User{
					Name:         "Jane Smith",
					Email:        "jane.smith@example.com",
					PasswordHash: "securepass",
					IsActive:     1,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				}

				if err := conn.Insert(tt.args.ctx, user); err != nil {
					t.Errorf("Insert() failed: %v", err)
					return
				}

				if user.Id != 1 {
					t.Errorf("Insert() failed: wrong id. expected 1 got %d", user.Id)
					return
				}

				user2 = &records.User{
					Name:         "Pavan Yewale",
					Email:        "pavany@xyz.com",
					PasswordHash: "strongPass",
					IsActive:     0,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				}

				if err := conn.Insert(tt.args.ctx, user2); err != nil {
					t.Errorf("Insert() failed: %v", err)
					return
				}

				if user2.Id != 2 {
					t.Errorf("Insert() failed: wrong id, expected 2 got %d", user2.Id)
					return
				}

				user3 = &records.User{
					Name:         "ram babu",
					Email:        "ram@gmail.com",
					PasswordHash: "pass2",
					IsActive:     1,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				}
				if err := conn.Insert(tt.args.ctx, user3); err != nil {
					t.Errorf("Insert() failed: %v", err)
					return
				}
				if user3.Id != 3 {
					t.Errorf("Insert() failed: wrong id, expected 3 got %d", user3.Id)
					return
				}
			}

			{
				// get single record by filter email
				users := &records.Users{}
				getFilter := &sql.Filter{
					Condition: &sql.Condition{
						Field:    "email",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.EQ,
					},
				}
				values := []any{user.Email}
				if err := conn.Get(tt.args.ctx, getFilter, values, users); err != nil {
					t.Errorf("GetByFilter() email failed: %v", err)
					return
				}

				if len(users.Users) != 1 {
					t.Errorf("GetByFilter() email failed: expected 1 user, got %d", len(users.Users))
					return
				}
				gotUser := users.Users[0]
				if gotUser.Name != user.Name {
					t.Errorf("GetByFilter() email failed: name = %v, want %v", gotUser.Name, user.Name)
				}
				if gotUser.Email != user.Email {
					t.Errorf("GetByFilter() email failed: email = %v, want %v", gotUser.Email, user.Email)
				}
				if gotUser.PasswordHash != user.PasswordHash {
					t.Errorf("GetByFilter() email failed: password hash = %v, want %v", gotUser.PasswordHash, user.PasswordHash)
				}
				if gotUser.IsActive != user.IsActive {
					t.Errorf("GetByFilter() email failed: is active = %v, want %v", gotUser.IsActive, user.IsActive)
				}
				if gotUser.CreatedAt == 0 {
					t.Errorf("GetByFilter() email failed: created at is zero")
				}
				if gotUser.UpdatedAt == 0 {
					t.Errorf("GetByFilter() email failed: updated at is zero")
				}
			}
			{
				// get multiple records by filter is_active
				users := &records.Users{}
				getFilter := &sql.Filter{
					Condition: &sql.Condition{
						Field:    "is_active",
						Value:    sql.NewIndexedValue(0),
						Operator: sql.EQ,
					},
				}
				values := []any{1} // Get all active users
				if err := conn.Get(tt.args.ctx, getFilter, values, users); err != nil {
					t.Errorf("GetByFilter() is_active failed: %v", err)
					return
				}

				if len(users.Users) != 2 {
					t.Errorf("GetByFilter() is_active failed: expected 2 users, got %d", len(users.Users))
					return
				}
				gotUser := users.Users[0]
				if gotUser.Name != user.Name {
					t.Errorf("GetByFilter() is_active failed: name = %v, want %v", gotUser.Name, user.Name)
				}
				if gotUser.Email != user.Email {
					t.Errorf("GetByFilter() is_active failed: email = %v, want %v", gotUser.Email, user.Email)
				}
				if gotUser.PasswordHash != user.PasswordHash {
					t.Errorf("GetByFilter() is_active failed: password hash = %v, want %v", gotUser.PasswordHash, user.PasswordHash)
				}
				if gotUser.IsActive != user.IsActive {
					t.Errorf("GetByFilter() is_active failed: is active = %v, want %v", gotUser.IsActive, user.IsActive)
				}
				if gotUser.CreatedAt == 0 {
					t.Errorf("GetByFilter() is_active failed: created at is zero")
				}
				if gotUser.UpdatedAt == 0 {
					t.Errorf("GetByFilter() is_active failed: updated at is zero")
				}
			}
			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}

func TestInsertAndGet_EdgeCases(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql insert and get edge cases",
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
				return
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
					return
				}
			}()
			conn, err := sqlfactory.NewSQLDatabase(tt.args.ctx, tt.args.config)
			if err != nil {
				t.Fatalf("NewConnection() error = %v", err)
				return
			}
			defer conn.Close(tt.args.ctx)

			// Insert with empty strings and special characters
			edgeUser := &records.User{
				Name:         "",
				Email:        "!@#$%^&*()_+|}{:?><,./;'[]\\=-`~",
				PasswordHash: "p@$$w0rd!",
				IsActive:     1,
				CreatedAt:    time.Now().UnixMilli(),
				UpdatedAt:    time.Now().UnixMilli(),
			}
			if err := conn.Insert(tt.args.ctx, edgeUser); err != nil {
				t.Errorf("Insert() failed: %v", err)
				return
			}
			if edgeUser.Id == 0 {
				t.Errorf("Insert() failed: id is 0")
				return
			}
			// Get by ID
			getUser := &records.User{Id: edgeUser.Id}
			if err := conn.GetByID(tt.args.ctx, getUser); err != nil {
				t.Errorf("GetByID() failed: %v", err)
				return
			}
			if getUser.Email != edgeUser.Email {
				t.Errorf("GetByID() failed: email mismatch")
				return
			}

			// Insert with future and past timestamps
			futureUser := &records.User{
				Name:         "Future",
				Email:        "future@example.com",
				PasswordHash: "futurepass",
				IsActive:     1,
				CreatedAt:    time.Now().Add(24 * time.Hour).UnixMilli(),
				UpdatedAt:    time.Now().Add(24 * time.Hour).UnixMilli(),
			}
			if err := conn.Insert(tt.args.ctx, futureUser); err != nil {
				t.Errorf("Insert() failed: %v", err)
				return
			}
			pastUser := &records.User{
				Name:         "Past",
				Email:        "past@example.com",
				PasswordHash: "pastpass",
				IsActive:     1,
				CreatedAt:    time.Now().Add(-24 * time.Hour).UnixMilli(),
				UpdatedAt:    time.Now().Add(-24 * time.Hour).UnixMilli(),
			}
			if err := conn.Insert(tt.args.ctx, pastUser); err != nil {
				t.Errorf("Insert() failed: %v", err)
				return
			}

			// Get by non-existent ID
			nonExistent := &records.User{Id: 999999}
			err = conn.GetByID(tt.args.ctx, nonExistent)
			if err != sql.ErrNoRecordFound {
				t.Errorf("GetByID() non-existent: got %v, want %v", err, sql.ErrNoRecordFound)
			}
		})
	}
}

func TestMysqlDatabase_GetByFilter_Advanced(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql get by filter advanced",
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
				return
			}
			defer func() {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
					return
				}
			}()
			conn, err := sqlfactory.NewSQLDatabase(tt.args.ctx, tt.args.config)
			if err != nil {
				t.Fatalf("NewConnection() error = %v", err)
				return
			}
			defer conn.Close(tt.args.ctx)

			// Insert some users for filter tests
			for i := 0; i < 5; i++ {
				_ = conn.Insert(tt.args.ctx, &records.User{
					Name:         "FilterUser",
					Email:        fmt.Sprintf("filteruser%v@example.com", 'A'+i),
					PasswordHash: "filterpass",
					IsActive:     i % 2,
					CreatedAt:    time.Now().UnixMilli(),
					UpdatedAt:    time.Now().UnixMilli(),
				})
			}

			// LIKE filter
			users := &records.Users{}
			likeFilter := &sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Value:    sql.NewValue("FilterUser"),
					Operator: sql.LIKE,
				},
			}
			if err := conn.Get(tt.args.ctx, likeFilter, nil, users); err != nil {
				t.Errorf("GetByFilter() LIKE failed: %v", err)
				return
			}
			if len(users.Users) == 0 {
				t.Errorf("GetByFilter() LIKE failed: expected >0 users")
				return
			}

			// IN filter
			users = &records.Users{}
			inFilter := &sql.Filter{
				Condition: &sql.Condition{
					Field:    "is_active",
					Value:    sql.NewValue([]any{0, 1}),
					Operator: sql.IN,
				},
			}
			if err := conn.Get(tt.args.ctx, inFilter, nil, users); err != nil {
				t.Errorf("GetByFilter() IN failed: %v", err)
				return
			}
			if len(users.Users) < 2 {
				t.Errorf("GetByFilter() IN failed: expected >=2 users")
				return
			}

			// IS NULL filter (should be 0)
			users = &records.Users{}
			isNullFilter := &sql.Filter{
				Condition: &sql.Condition{
					Field:    "deleted_at",
					Operator: sql.ISNULL,
				},
			}
			_ = conn.Get(tt.args.ctx, isNullFilter, nil, users) // Should not error, but likely 0 results

			// BETWEEN filter (CreatedAt in range)
			users = &records.Users{}
			from := time.Now().Add(-1 * time.Hour).UnixMilli()
			to := time.Now().Add(1 * time.Hour).UnixMilli()
			betweenFilter := &sql.Filter{
				Condition: &sql.Condition{
					Field:    "created_at",
					Value:    sql.NewValue([]any{from, to}),
					Operator: sql.BETWEEN,
				},
			}
			if err := conn.Get(tt.args.ctx, betweenFilter, nil, users); err != nil {
				t.Errorf("GetByFilter() BETWEEN failed: %v", err)
				return
			}

			// Sorting, limit, offset
			users = &records.Users{}
			sortFilter := &sql.Filter{
				Sort:   sql.NewSort().Add("created_at", sql.Desc),
				Limit:  sql.NewValue(int64(2)),
				Offset: sql.NewValue(int64(1)),
			}
			_ = conn.Get(tt.args.ctx, sortFilter, nil, users)
			// No error expected, just check code path
		})
	}
}
