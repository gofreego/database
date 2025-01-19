package postgresql

import (
	"context"
	"fmt"

	"database/sql"

	"github.com/gofreego/database/database/dberrors"

	"github.com/gofreego/goutils/logger"
	_ "github.com/lib/pq" // import the PostgreSQL driver
)

type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	SSLMode  string // disable/require/verify-ca/verify-full  ## Note: disable by default
}

func NewConnection(ctx context.Context, conf *Config) (*sql.DB, error) {
	if conf.SSLMode == "" {
		conf.SSLMode = "disable"
	}
	// Create the connection string
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		conf.Host, conf.Port, conf.Username, conf.Password, conf.Database, conf.SSLMode)

	// Open a new database connection
	db, err := sql.Open("postgres", connString)
	if err != nil {
		logger.Error(ctx, "Database::Postgresql::Connection failed, Err: %s", err.Error())
		return nil, dberrors.NewError(dberrors.ErrConnectionClosed, "Connection failed", err)
	}

	// Ping the database to check the connection
	err = db.PingContext(ctx)
	if err != nil {
		logger.Error(ctx, "Database::Postgresql::Ping failed, Err: %s", err.Error())
		return nil, dberrors.ParseSQLError("Ping failed", err)
	}
	return db, nil
}
