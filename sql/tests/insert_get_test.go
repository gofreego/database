package tests

import (
	"context"
	"testing"
	"time"

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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newFunction(t, tt)
		})
	}
}

func newFunction(t *testing.T, tt testCase) bool {

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
		return true
	}
	if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
		t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
	}

	user := &records.User{
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
