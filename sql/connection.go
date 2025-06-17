package sql

import (
	"context"

	"github.com/gofreego/database/sql/impls/mssql"
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/sqlerror"
)

type DBName string

const (
	PostgreSQL DBName = "postgresql"
	MySQL      DBName = "mysql"
	MSSQL      DBName = "mssql"
)

type SQLDatabase interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

type Config struct {
	Name       DBName             `yaml:"Name" json:"Name"`
	PostgreSQL *postgresql.Config `yaml:"PostgreSQL" json:"PostgreSQL"`
	MySQL      *mysql.Config      `yaml:"MySQL" json:"MySQL"`
	MSSQL      *mssql.Config      `yaml:"MSSQL" json:"MSSQL"`
}

func NewSQLDatabase(ctx context.Context, config *Config) (SQLDatabase, error) {
	switch config.Name {
	case PostgreSQL:
		return postgresql.NewPostgresqlDatabase(ctx, config.PostgreSQL)
	case MySQL:
		return mysql.NewMysqlDatabase(ctx, config.MySQL)
	case MSSQL:
		return mssql.NewMssqlDatabase(ctx, config.MSSQL)
	default:
		return nil, sqlerror.ErrInvalidConfig
	}
}
