package migrator

import (
	"context"
	"database/sql"
	"errors"

	internalmysql "github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/goutils/logger"
	"github.com/golang-migrate/migrate/v4"
	migrationdatabase "github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/file"
)

type Action string

const (
	ACTION_UP   Action = "UP"
	ACTION_DOWN Action = "DOWN"
)

type Config struct {
	Database  sqlfactory.Config
	FilesPath string `yaml:"FilesPath"`
	Action    Action `yaml:"Action"` // UP | DOWN
}

type Migrator struct {
	conf *Config
	m    *migrate.Migrate
}

func NewMigrator(ctx context.Context, conf *Config) *Migrator {
	return &Migrator{conf: conf}
}

func (app *Migrator) Run(ctx context.Context) error {
	logger.Info(ctx, "MigrationScript started for %s with action: %s", app.conf.Database.Name, app.conf.Action)
	fileSource, err := (&file.File{}).Open(app.conf.FilesPath)
	if err != nil {
		logger.Error(ctx, "error opening migration source directory:%s, error: %s", app.conf.FilesPath, err.Error())
		return err
	}
	defer fileSource.Close()
	conn, db, dbname, err := getDBDriver(ctx, &app.conf.Database)
	if err != nil {
		return err
	}
	if app.conf.Action == ACTION_DOWN {
		result, err := conn.Exec("update schema_migrations  set dirty  = false where dirty = true;")
		if err != nil {
			logger.Error(ctx, "failed to set dirty false, Err: %s", err.Error())
		}
		_, err = result.RowsAffected()
		if err != nil {
			logger.Error(ctx, "failed to retrive rows affected, Err: %s", err.Error())
		}
	}

	// Create a new migrate instance
	app.m, err = migrate.NewWithInstance("source", fileSource, dbname, db)
	if err != nil {
		logger.Error(ctx, "error creating migrate instance: %v", err)
		return err
	}

	switch app.conf.Action {
	case ACTION_UP:
		// Apply the migration
		if err := app.m.Up(); err != nil {
			if err == migrate.ErrNoChange {
				logger.Info(ctx, "no change")
				return nil
			}
			logger.Error(ctx, "error applying up migration: %v", err)
			return err
		}
	case ACTION_DOWN:
		if err := app.m.Down(); err != nil {
			if err == migrate.ErrNoChange {
				logger.Info(ctx, "no change")
				return nil
			}
			logger.Error(ctx, "error applying down migration: %v", err)
			return err
		}
	default:
		logger.Error(ctx, "invalid action current: `%s` Expected : `%s` | `%s`", app.conf.Action, ACTION_UP, ACTION_DOWN)
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

func getDBDriver(ctx context.Context, conf *sqlfactory.Config) (*sql.DB, migrationdatabase.Driver, string, error) {
	switch conf.Name {
	case sqlfactory.PostgreSQL:
		conn, err := postgresql.NewConnection(ctx, conf.PostgreSQL)
		if err != nil {
			logger.Error(ctx, "error creating postgres connection: %v", err)
			return nil, nil, "", err
		}
		driver, err := postgres.WithInstance(conn, &postgres.Config{DatabaseName: conf.PostgreSQL.Database})
		if err != nil {
			logger.Error(ctx, "error creating postgres driver: %v", err)
			return nil, nil, "", err
		}
		return conn, driver, conf.PostgreSQL.Database, nil
	case sqlfactory.MySQL:
		conn, err := internalmysql.NewConnection(ctx, conf.MySQL)
		if err != nil {
			logger.Error(ctx, "error creating mysql connection: %v", err)
			return nil, nil, "", err
		}
		driver, err := mysql.WithInstance(conn, &mysql.Config{DatabaseName: conf.MySQL.Database})
		if err != nil {
			logger.Error(ctx, "error creating mysql driver: %v", err)
			return nil, nil, "", err
		}
		return conn, driver, conf.MySQL.Database, nil
	default:
		logger.Error(ctx, "invalid repository name: current: %s , Expected: %s", conf.Name, sqlfactory.PostgreSQL)
		return nil, nil, "", errors.New("invalid repository name")
	}
}
