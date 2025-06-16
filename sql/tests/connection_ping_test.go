package sql

import (
	"context"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
)

/*
Note: please make sure the database is running before running the test
use `make setup-db` to start the database
*/

func TestNewConnection(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *sql.Config
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
				config: &sql.Config{
					Name: sql.DBNamePostgreSQL,
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
				config: &sql.Config{
					Name: sql.DBNameMySQL,
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
			conn, err := sql.NewConnection(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
