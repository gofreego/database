package mysql

import (
	"context"
	driver "database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofreego/database/sql/internal"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

type MysqlDatabase struct {
	db                 *driver.DB
	preparedStatements internal.PreparedStatements
}

func NewConnection(ctx context.Context, config *Config) (*driver.DB, error) {
	db, err := driver.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", config.User, config.Password, config.Host, config.Port, config.Database))
	if err != nil {
		return nil, err
	}
	return db, nil
}

func NewMysqlDatabase(ctx context.Context, config *Config) (*MysqlDatabase, error) {
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &MysqlDatabase{
		db:                 conn,
		preparedStatements: internal.NewPreparedStatements(),
	}, nil
}

func (c *MysqlDatabase) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *MysqlDatabase) Close(ctx context.Context) error {
	c.preparedStatements.Close()
	return internal.HandleError(c.db.Close())
}
