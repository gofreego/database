package mssql

import (
	"context"
	driver "database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gofreego/database/sql/impls/unimplemented"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

type MssqlDatabase struct {
	db *driver.DB
	unimplemented.Unimplemented
}

func NewConnection(ctx context.Context, config *Config) (*driver.DB, error) {
	db, err := driver.Open("mssql", fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s", config.Host, config.Port, config.User, config.Password, config.Database))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewMssqlDatabase(ctx context.Context, config *Config) (*MssqlDatabase, error) {
	db, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &MssqlDatabase{db: db}, nil
}

func (c *MssqlDatabase) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *MssqlDatabase) Close(ctx context.Context) error {
	return c.db.Close()
}
