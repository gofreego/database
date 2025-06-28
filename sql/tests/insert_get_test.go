package tests

import (
	"context"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/sqlfactory"
)

/*
Note: please make sure the database is running before running the test
use `make setup-db` to start the database
*/

type User struct {
	Id   int64  `sql:"id"`
	Name string `sql:"name"`
	Age  int    `sql:"age"`
}

// Columns implements sql.Record.
func (u *User) Columns() []string {
	return []string{"name", "age"}
}

// ID implements sql.Record.
func (u *User) ID() int64 {
	return u.Id
}

// Scan implements sql.Record.
func (u *User) Scan(row sql.Row) error {
	return row.Scan(&u.Name, &u.Age)
}

// SetID implements sql.Record.
func (u *User) SetID(id int64) {
	u.Id = id
}

// Table implements sql.Record.
func (u *User) Table() *sql.Table {
	return &sql.Table{
		Name: "users",
	}
}

// Values implements sql.Record.
func (u *User) Values() []any {
	return []any{u.Name, u.Age}
}

func TestInsertAndGet(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *sqlfactory.Config
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		pingErr bool
	}{
		{
			name: "postgresql",
			args: args{
				ctx: context.Background(),
				config: &sqlfactory.Config{
					Name: sqlfactory.PostgreSQL,
					PostgreSQL: &postgresql.Config{
						Host:     "localhost",
						Port:     5432,
						User:     "root",
						Password: "root@1234",
						Database: "postgres",
					},
				},
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "mysql",
			args: args{
				ctx: context.Background(),
				config: &sqlfactory.Config{
					Name: sqlfactory.MySQL,
					MySQL: &mysql.Config{
						Host:     "localhost",
						Port:     3306,
						User:     "root",
						Password: "root@1234",
						Database: "mysql",
					},
				},
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
			conn, err := sqlfactory.NewSQLDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}

			user := &User{
				Name: "John Doe",
				Age:  20,
			}

			if err := conn.Insert(tt.args.ctx, user); err != nil {
				t.Errorf("Insert() failed: %v", err)
			}
			user.Age = 0
			user.Name = ""
			if err := conn.GetByID(tt.args.ctx, user); err != nil {
				t.Errorf("GetByID() failed: %v", err)
			}
			if user.Age != 20 {
				t.Errorf("GetByID() failed: %v", err)
			}
			if user.Name != "John Doe" {
				t.Errorf("GetByID() failed: %v", err)
			}
			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
			if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
				t.Errorf("MigrationDown() failed: %v", err)
			}
		})
	}
}
