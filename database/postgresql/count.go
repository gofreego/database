package postgresql

import (
	"context"

	"database/sql"

	"github.com/gofreego/database/database/dbcommon"
	"github.com/gofreego/database/database/dberrors"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) Count(ctx context.Context, record dbcommon.SQLRecord, filter dbcommon.Filter, options ...any) (int, error) {
	prepareName := dbcommon.GetPrepareName(options)
	var row *sql.Row
	var err error
	var values []interface{}
	var query string

	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			query, values = generateCountQuery(parseTableName(record.Table()), filter)
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::Count::Prepare statement failed for name %s, table %s, Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return 0, dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		row = d.preparedStatements[prepareName].QueryRowContext(ctx, values...)
	} else {
		query, values := generateCountQuery(parseTableName(record.Table()), filter)
		row = d.conn.QueryRowContext(ctx, query, values...)
	}

	if row.Err() != nil {
		logger.Error(ctx, "Database::PostgreSQL::Count::Count failed for table %s, Err:%s", parseTableName(record.Table()), row.Err().Error())
		return 0, dberrors.ParseSQLError("Count failed for table "+parseTableName(record.Table()), row.Err())
	}
	var count int
	err = row.Scan(&count)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::Count::Scan failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return 0, dberrors.ParseSQLError("Scan failed for table "+parseTableName(record.Table()), err)
	}
	return count, nil
}

/*
 helper function to generate query for count
*/

func generateCountQuery(tableName string, filter dbcommon.Filter) (string, []interface{}) {
	query := "SELECT COUNT(1) FROM " + tableName
	var values []interface{}
	if filter != nil {
		valueNumber := 1
		condition, condValues := parseCondition(filter.Condition(), &valueNumber)
		if condition != "" {
			query += " WHERE " + condition
			values = append(values, condValues...)
		}
	}
	return query, values
}
