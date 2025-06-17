package migrator

import (
	"context"
	"database/sql"

	sqldb "github.com/gofreego/database/sql"
	internalmysql "github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/goutils/logger"
	"github.com/golang-migrate/migrate/v4"
	migrationdatabase "github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/file"
)

const (
	ACTION_UP   = "UP"
	ACTION_DOWN = "DOWN"
)

type Config struct {
	Database  sqldb.Config
	FilesPath string `yaml:"FilesPath"`
	Action    string `yaml:"Action"` // UP | DOWN
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
	fileSource, err := (&file.File{}).Open(app.conf.FilesPath)
	if err != nil {
		logger.Panic(ctx, "error opening migration source directory: %v", err)
		return err
	}
	defer fileSource.Close()
	conn, db, dbname := getDBDriver(ctx, &app.conf.Database)
	if app.conf.Action == ACTION_DOWN {
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

	switch app.conf.Action {
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
		logger.Panic(ctx, "invalid action current: `%s` Expected : `%s` | `%s`", app.conf.Action, ACTION_UP, ACTION_DOWN)
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

func getDBDriver(ctx context.Context, conf *sqldb.Config) (*sql.DB, migrationdatabase.Driver, string) {
	switch conf.Name {
	case sqldb.PostgreSQL:
		conn, err := postgresql.NewConnection(ctx, conf.PostgreSQL)
		if err != nil {
			logger.Panic(ctx, "error creating postgres connection: %v", err)
		}
		driver, err := postgres.WithInstance(conn, &postgres.Config{DatabaseName: conf.PostgreSQL.Database})
		if err != nil {
			logger.Panic(ctx, "error creating postgres driver: %v", err)
		}
		return conn, driver, conf.PostgreSQL.Database
	case sqldb.MySQL:
		conn, err := internalmysql.NewConnection(ctx, conf.MySQL)
		if err != nil {
			logger.Panic(ctx, "error creating mysql connection: %v", err)
		}
		driver, err := mysql.WithInstance(conn, &mysql.Config{DatabaseName: conf.MySQL.Database})
		if err != nil {
			logger.Panic(ctx, "error creating mysql driver: %v", err)
		}
		return conn, driver, conf.MySQL.Database
	default:
		logger.Panic(ctx, "invalid repository name: current: %s , Expected: %s", conf.Name, sqldb.PostgreSQL)
		return nil, nil, ""
	}
}
