package database

import (
	"context"
	"database/database/dbcommon"
	"database/database/dberrors"
	"database/database/postgresql"
	"fmt"

	"github.com/gofreego/goutils/logger"
)

type Database interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	Insert(ctx context.Context, record dbcommon.Record, options ...any) error
	UpdateByID(ctx context.Context, record dbcommon.Record, options ...any) error
	UpdateByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error)
	DeleteByID(ctx context.Context, record dbcommon.Record, options ...any) error
	DeleteByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error)
	FindOneByID(ctx context.Context, record dbcommon.Record, options ...any) error
	FindOneByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) error
	FindAll(ctx context.Context, record dbcommon.Records, filter dbcommon.Filter, options ...any) error
	Count(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int, error)
	Aggregate(ctx context.Context, record dbcommon.AggregationRecords, filter dbcommon.Filter, options ...any) error
}

const (
	PostgreSQL = "PostgreSQL"
)

type Config struct {
	Name        string
	Logger      logger.Config
	PostgresSQL *postgresql.Config
}

func withDefaultValues(loggerConf logger.Config) logger.Config {
	if loggerConf.AppName == "" {
		loggerConf.AppName = "Database"
	}
	if loggerConf.Level == "" {
		loggerConf.Level = "debug"
	}
	if loggerConf.Build == "" {
		loggerConf.Build = "development"
	}
	return loggerConf
}

func NewDatabase(ctx context.Context, config Config) (Database, error) {
	withDefaultValues(config.Logger).InitiateLogger()
	switch config.Name {
	case PostgreSQL:
		return postgresql.NewDatabase(ctx, config.PostgresSQL)
	default:
		logger.Error(ctx, "Database::NewDatabase failed, Err: invalid database name: %s, Expected", config.Name, PostgreSQL)
		return nil, dberrors.NewError(dberrors.ErrInvalidConfig, "Invalid Database Name", fmt.Errorf("invalid database name: %s, Expected: %s", config.Name, PostgreSQL))
	}
}
