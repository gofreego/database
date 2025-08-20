package mysql

import (
	"context"
	driver "database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofreego/database/sql/impls/common"
	"github.com/gofreego/database/sql/impls/mysql/parser"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

func NewConnection(ctx context.Context, config *Config) (*driver.DB, error) {
	db, err := driver.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", config.User, config.Password, config.Host, config.Port, config.Database))
	if err != nil {
		return nil, err
	}
	return db, nil
}

type MysqlDatabase struct {
	*common.Executor
}

func NewMysqlDatabase(ctx context.Context, config *Config) (*MysqlDatabase, error) {
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &MysqlDatabase{
		Executor: common.NewExecutor(conn, parser.NewParser()),
	}, nil
}
