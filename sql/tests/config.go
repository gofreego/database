package tests

import (
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/sqlfactory"
)

var (
	mysqlConfig = sqlfactory.Config{
		Name: sqlfactory.MySQL,
		MySQL: &mysql.Config{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "root@1234",
			Database: "mysql",
		},
	}

	postgresqlConfig = sqlfactory.Config{
		Name: sqlfactory.PostgreSQL,
		PostgreSQL: &postgresql.Config{
			Host:     "localhost",
			Port:     5432,
			User:     "root",
			Password: "root@1234",
			Database: "postgres",
		},
	}
)
