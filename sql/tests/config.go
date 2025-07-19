package tests

import (
	"context"

	"github.com/gofreego/database/sql/impls/mssql"
	"github.com/gofreego/database/sql/impls/mysql"
	"github.com/gofreego/database/sql/impls/postgresql"
	"github.com/gofreego/database/sql/sqlfactory"
)

type args struct {
	ctx    context.Context
	config *sqlfactory.Config
}
type testCase struct {
	name    string
	args    args
	wantErr bool
	pingErr bool
}

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

	mssqlConfig = sqlfactory.Config{
		Name: sqlfactory.MSSQL,
		MSSQL: &mssql.Config{
			Host:     "localhost",
			Port:     1433,
			User:     "sa",
			Password: "root@1234",
			Database: "master",
		},
	}

	tests = []testCase{
		{
			name: "mysql",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
			},
		},
	}
)
