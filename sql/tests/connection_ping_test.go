package tests

import (
	"context"
	"testing"

	"github.com/gofreego/database/sql/sqlfactory"
)

/*
Note: please make sure the database is running before running the test
use `make setup-db` to start the database
*/

func TestNewConnection(t *testing.T) {
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
			name: "Ping Postgresql",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "Ping Mysql",
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
			conn, err := sqlfactory.NewDatabase(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err := conn.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := conn.Close(tt.args.ctx); err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}
