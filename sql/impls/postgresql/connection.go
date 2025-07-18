package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gofreego/database/sql/impls/common"
	"github.com/gofreego/database/sql/impls/postgresql/parser"
	_ "github.com/lib/pq"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

type PostgresqlDatabase struct {
	*common.Executor
}

func NewConnection(ctx context.Context, config *Config) (*sql.DB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", config.Host, config.Port, config.User, config.Password, config.Database))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewPostgresqlDatabase(ctx context.Context, config *Config) (*PostgresqlDatabase, error) {
	db, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &PostgresqlDatabase{
		Executor: common.NewExecutor(db, parser.NewParser()),
	}, nil
}
