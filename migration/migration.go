package migration

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gofreego/database/database"
	"github.com/gofreego/database/database/postgresql"
	"github.com/gofreego/goutils/logger"
	"github.com/golang-migrate/migrate/v4"
	migrationdatabase "github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/file"
)

const (
	ACTION_UP   = "UP"
	ACTION_DOWN = "DOWN"
)

type MigrationConfig struct {
	FilesPath string `yaml:"FilesPath"`
	Action    string `yaml:"Action"` // UP | DOWN
}

type Config interface {
	GetMigrationConfig() *MigrationConfig
	GetDatabaseConfig() *database.Config
}

type Migrator struct {
	conf Config
	m    *migrate.Migrate
}

func NewMigrator(ctx context.Context, conf Config) *Migrator {
	return &Migrator{conf: conf}
}

func (app *Migrator) Run(ctx context.Context) error {
	logger.Info(ctx, "MigrationScript started...")
	if app.conf.GetMigrationConfig() == nil || app.conf.GetDatabaseConfig() == nil {
		logger.Panic(ctx, "MigrationScript: invalid config, migration or database config is nil")
		return fmt.Errorf("MigrationScript: invalid config, migration or database config is nil")

	}
	fileSource, err := (&file.File{}).Open(app.conf.GetMigrationConfig().FilesPath)
	if err != nil {
		logger.Panic(ctx, "error opening migration source: %v", err)
		return err
	}
	defer fileSource.Close()
	conn, db, dbname := getDBDriver(ctx, app.conf.GetDatabaseConfig())
	if app.conf.GetMigrationConfig().Action == ACTION_DOWN {
		result, err := conn.Exec("update schema_migrations  set dirty  = false where dirty = true;")
		if err != nil {
			logger.Panic(ctx, "failed to set dirty false, Err: %s", err.Error())
		}
		_, err = result.RowsAffected()
		if err != nil {
			logger.Panic(ctx, "failed to retrive rows affected, Err: %s", err.Error())
		}
	}

	// Create a new migrate instance
	app.m, err = migrate.NewWithInstance("source", fileSource, dbname, db)
	if err != nil {
		logger.Panic(ctx, "error creating migrate instance: %v", err)
		return err
	}

	switch app.conf.GetMigrationConfig().Action {
	case ACTION_UP:
		// Apply the migration
		if err := app.m.Up(); err != nil {
			if err == migrate.ErrNoChange {
				logger.Info(ctx, "no change")
				return nil
			}
			logger.Panic(ctx, "error applying up migration: %v", err)
			return err
		}
	case ACTION_DOWN:
		if err := app.m.Down(); err != nil {
			if err == migrate.ErrNoChange {
				logger.Info(ctx, "no change")
				return nil
			}
			logger.Panic(ctx, "error applying down migration: %v", err)
			return err
		}
	default:
		logger.Panic(ctx, "invalid action current: `%s` Expected : `%s` | `%s`", app.conf.GetMigrationConfig().Action, ACTION_UP, ACTION_DOWN)
	}
	logger.Info(ctx, "Migration applied successfully!")
	return nil
}

func (app *Migrator) Shutdown(ctx context.Context) {
	app.m.GracefulStop <- true
}

func (app *Migrator) Name() string {
	return "MigrationScript"
}

func getDBDriver(ctx context.Context, conf *database.Config) (*sql.DB, migrationdatabase.Driver, string) {
	switch conf.Name {
	case database.PostgreSQL:
		conn, err := postgresql.NewConnection(ctx, conf.PostgreSQL)
		if err != nil {
			logger.Panic(ctx, "error creating postgres connection: %v", err)
		}
		driver, err := postgres.WithInstance(conn, &postgres.Config{DatabaseName: conf.PostgreSQL.Database})
		if err != nil {
			logger.Panic(ctx, "error creating postgres driver: %v", err)
		}
		return conn, driver, conf.PostgreSQL.Database
	}

	logger.Panic(ctx, "invalid repository name: current: %s , Expected: %s", conf.Name, database.PostgreSQL)
	return nil, nil, ""
}
