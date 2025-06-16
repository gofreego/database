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
	DBNamePostgreSQL DBName = "postgresql"
	DBNameMySQL      DBName = "mysql"
	DBNameMSSQL      DBName = "mssql"
)

type Connection interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

type Config struct {
	Name       DBName             `yaml:"Name" json:"Name"`
	PostgreSQL *postgresql.Config `yaml:"PostgreSQL" json:"PostgreSQL"`
	MySQL      *mysql.Config      `yaml:"MySQL" json:"MySQL"`
	MSSQL      *mssql.Config      `yaml:"MSSQL" json:"MSSQL"`
}

func NewConnection(ctx context.Context, config *Config) (Connection, error) {
	switch config.Name {
	case DBNamePostgreSQL:
		return postgresql.NewConnection(ctx, config.PostgreSQL)
	case DBNameMySQL:
		return mysql.NewConnection(ctx, config.MySQL)
	case DBNameMSSQL:
		return mssql.NewConnection(ctx, config.MSSQL)
	default:
		return nil, sqlerror.ErrInvalidConfig
	}
}
