package tests

import (
	"context"
	dbsql "database/sql"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/impls/mssql"
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/migrator"
	"github.com/gofreego/database/sql/sqlfactory"
)

// CleanDirtyState cleans up any dirty migration state in the database
func CleanDirtyState(ctx context.Context, cfg *sqlfactory.Config) error {
	var conn *dbsql.DB
	var err error

	switch cfg.Name {
	case sqlfactory.PostgreSQL:
		conn, err = postgresql.NewConnection(ctx, cfg.PostgreSQL)
		if err != nil {
			return err
		}
	case sqlfactory.MySQL:
		conn, err = mysql.NewConnection(ctx, cfg.MySQL)
		if err != nil {
			return err
		}
	case sqlfactory.MSSQL:
		conn, err = mssql.NewConnection(ctx, cfg.MSSQL)
		if err != nil {
			return err
		}
	default:
		return sql.ErrInvalidConfig
	}
	defer conn.Close()

	var cleanSQL string
	if cfg.Name == sqlfactory.MSSQL {
		cleanSQL = "UPDATE schema_migrations SET dirty = 0 WHERE dirty = 1"
	} else {
		cleanSQL = "UPDATE schema_migrations SET dirty = false WHERE dirty = true"
	}
	_, err = conn.ExecContext(ctx, cleanSQL)
	return err
}

func MigrationUP(ctx context.Context, cfg *sqlfactory.Config) error {
	// Clean up any dirty state before running migrations
	if err := CleanDirtyState(ctx, cfg); err != nil {
		// If schema_migrations table doesn't exist, that's fine - it means no migrations have been run yet
		// We'll ignore this error and continue
	}

	filesPath := "./migrations/mysql"
	switch cfg.Name {
	case sqlfactory.PostgreSQL:
		filesPath = "./migrations/postgresql"
	case sqlfactory.MSSQL:
		filesPath = "./migrations/mssql"
	}

	migrator := migrator.NewMigrator(ctx, &migrator.Config{
		Database:  *cfg,
		FilesPath: filesPath,
		Action:    migrator.ACTION_UP,
	})
	return migrator.Run(ctx)
}

func MigrationDown(ctx context.Context, cfg *sqlfactory.Config) error {
	filesPath := "./migrations/mysql"
	switch cfg.Name {
	case sqlfactory.PostgreSQL:
		filesPath = "./migrations/postgresql"
	case sqlfactory.MSSQL:
		filesPath = "./migrations/mssql"
	}

	migrator := migrator.NewMigrator(ctx, &migrator.Config{
		Database:  *cfg,
		FilesPath: filesPath,
		Action:    migrator.ACTION_DOWN,
	})
	return migrator.Run(ctx)
}

func TestMigration(t *testing.T) {
	type args struct {
		ctx    context.Context
		config *sqlfactory.Config
		action migrator.Action
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		pingErr bool
	}{
		{
			name: "postgresql up",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
				action: migrator.ACTION_UP,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "postgresql down",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
				action: migrator.ACTION_DOWN,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "mysql up",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
				action: migrator.ACTION_UP,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "mysql down",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
				action: migrator.ACTION_DOWN,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "mssql up",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
				action: migrator.ACTION_UP,
			},
			wantErr: false,
			pingErr: false,
		},
		{
			name: "mssql down",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
				action: migrator.ACTION_DOWN,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.action == migrator.ACTION_UP {
				if err := MigrationUP(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationUP() failed: %v", err)
				}
			} else {
				if err := MigrationDown(tt.args.ctx, tt.args.config); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}
		})
	}
}
