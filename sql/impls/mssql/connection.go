package mssql

import (
	"context"
	driver "database/sql"
	"fmt"

	"github.com/gofreego/database/sql/impls/common"
	"github.com/gofreego/database/sql/impls/mssql/parser"
	"github.com/gofreego/database/sql/internal"
	_ "github.com/microsoft/go-mssqldb"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

func NewConnection(ctx context.Context, config *Config) (*driver.DB, error) {
	db, err := driver.Open("sqlserver", fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s", config.Host, config.Port, config.User, config.Password, config.Database))
	if err != nil {
		return nil, err
	}
	return db, nil
}

type MssqlDatabase struct {
	db     *driver.DB
	parser common.Parser
	*common.Executor
	prepared internal.PreparedStatements
}

func NewMssqlDatabase(ctx context.Context, config *Config) (*MssqlDatabase, error) {
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &MssqlDatabase{
		Executor: common.NewExecutor(conn, parser.NewParser()),
		db:       conn,
		parser:   parser.NewParser(),
		prepared: internal.NewPreparedStatements(),
	}, nil
}
