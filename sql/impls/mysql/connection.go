package mysql

import (
	"context"
	db "database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofreego/database/sql/impls/unimplemented"
)

type Config struct {
	Host     string `yaml:"Host" json:"Host"`
	Port     int    `yaml:"Port" json:"Port"`
	User     string `yaml:"User" json:"User"`
	Password string `yaml:"Password" json:"Password"`
	Database string `yaml:"Database" json:"Database"`
}

type MysqlDatabase struct {
	db                 *db.DB
	preparedStatements map[string]*db.Stmt
	unimplemented.Unimplemented
}

func NewConnection(ctx context.Context, config *Config) (*db.DB, error) {
	db, err := db.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", config.User, config.Password, config.Host, config.Port, config.Database))
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
		preparedStatements: make(map[string]*db.Stmt),
	}, nil
}

func (c *MysqlDatabase) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *MysqlDatabase) Close(ctx context.Context) error {
	for _, stmt := range c.preparedStatements {
		err := stmt.Close()
		if err != nil {
			return handleError(err)
		}
	}
	return handleError(c.db.Close())
}
