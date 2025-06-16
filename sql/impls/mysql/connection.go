package mysql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

type Connection struct {
	db *sql.DB
}

func NewConnection(ctx context.Context, config *Config) (*Connection, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.User, config.Password, config.Host, config.Port, config.Database))
	if err != nil {
		return nil, err
	}
	return &Connection{db: db}, nil
}

func (c *Connection) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Connection) Close(ctx context.Context) error {
	return c.db.Close()
}
