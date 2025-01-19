package database

import (
	"context"
	"fmt"

	"github.com/gofreego/database/database/dbcommon"
	"github.com/gofreego/database/database/dberrors"
	"github.com/gofreego/database/database/postgresql"

	"github.com/gofreego/goutils/logger"
)

type Database interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	Insert(ctx context.Context, record dbcommon.Record, options ...any) error
	InsertMany(ctx context.Context, records []dbcommon.Record, options ...any) error
	UpdateByID(ctx context.Context, record dbcommon.Record, options ...any) error
	UpdateByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error)
	DeleteByID(ctx context.Context, record dbcommon.Record, options ...any) error
	SoftDeleteByID(ctx context.Context, record dbcommon.Record, options ...any) error
	DeleteByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error)
	FindOneByID(ctx context.Context, record dbcommon.Record, options ...any) error
	FindOneByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) error
	FindAll(ctx context.Context, record dbcommon.Records, filter dbcommon.Filter, options ...any) error
	Count(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int, error)
	Aggregate(ctx context.Context, record dbcommon.AggregationRecords, filter dbcommon.Aggregator, options ...any) error
}

const (
	PostgreSQL = "PostgreSQL"
)

type Config struct {
	Name       string             `yaml:"Name"`
	Logger     *logger.Config     `yaml:"Logger"`
	PostgreSQL *postgresql.Config `yaml:"PostgreSQL"`
}

func NewDatabase(ctx context.Context, config *Config) (Database, error) {
	if config.Logger != nil {
		config.Logger.InitiateLogger()
	}
	switch config.Name {
	case PostgreSQL:
		return postgresql.NewDatabase(ctx, config.PostgreSQL)
	default:
		return nil, dberrors.NewError(dberrors.ErrInvalidConfig, "Invalid Database Name", fmt.Errorf("invalid database name: `%s`, Expected: `%s`", config.Name, PostgreSQL))
	}
}
