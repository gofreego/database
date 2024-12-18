package postgresql

import (
	"context"
	"database/sql"
	"openauth/database/database/dbcommon"
	"openauth/database/database/dberrors"
	"strconv"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) UpdateByID(ctx context.Context, record dbcommon.Record, options ...any) error {
	columns, values := record.UpdateColumnsValues()
	prepareName := dbcommon.GetPrepareName(options)
	values = append(values, record.ID())
	var result sql.Result
	var err error
	if prepareName != "" {
		stmt, ok := d.preparedStatements[prepareName]
		if !ok {
			// prepare statement
			stmt, err = d.conn.PrepareContext(ctx, generateUpdateByIDQuery(record.TableName(), columns))
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::UpdateByID::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute statement
		result, err = stmt.ExecContext(ctx, values...)
	} else {
		result, err = d.conn.ExecContext(ctx, generateUpdateByIDQuery(record.TableName(), columns), values...)
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::UpdateByID::Update failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("Update failed for table "+record.TableName(), err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::UpdateByID::RowsAffected failed for table %s, Err:%s", record.TableName(), err.Error())
		return dberrors.ParseSQLError("RowsAffected failed for table "+record.TableName(), err)
	}
	if count == 0 {
		return dberrors.NewError(dberrors.ErrRecordNotFound, "Record not found", nil)
	}
	return nil
}

func (d *Database) UpdateByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error) {
	columns, values := record.UpdateColumnsValues()
	prepareName := dbcommon.GetPrepareName(options)
	var result sql.Result
	var err error
	if prepareName != "" {
		stmt, ok := d.preparedStatements[prepareName]
		if !ok {
			// prepare statement
			query, filterValues := generateUpdateByFilterQuery(record.TableName(), columns, filter)
			values = append(values, filterValues...)
			stmt, err = d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::UpdateByFilter::Prepare statement failed for name %s, table %s, Err:%s", prepareName, record.TableName(), err.Error())
				return 0, dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+record.TableName(), err)
			}
			d.preparedStatements[prepareName] = stmt
		} else {
			filterValues := getValues(filter.Condition())
			values = append(values, filterValues...)
		}
		// execute statement
		result, err = stmt.ExecContext(ctx, values...)
	} else {
		query, filterValues := generateUpdateByFilterQuery(record.TableName(), columns, filter)
		values = append(values, filterValues...)
		result, err = d.conn.ExecContext(ctx, query, values...)
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::UpdateByFilter::Update failed for table %s, Err:%s", record.TableName(), err.Error())
		return 0, dberrors.ParseSQLError("Update failed for table "+record.TableName(), err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::UpdateByFilter::RowsAffected failed for table %s, Err:%s", record.TableName(), err.Error())
		return 0, dberrors.ParseSQLError("RowsAffected failed for table "+record.TableName(), err)
	}
	return count, nil
}

/*
	helper function to get SET clause for update query
*/

func getSetClause(columns []string) string {
	setClause := ""
	for i, column := range columns {
		if i > 0 {
			setClause += ","
		}
		setClause += column + "=$" + strconv.Itoa(i+1)
	}
	return setClause
}

/*
	helper function to generate update query
*/

func generateUpdateByIDQuery(table string, columns []string) string {
	setClause := getSetClause(columns)
	return "UPDATE " + table + " SET " + setClause + " WHERE id=$" + strconv.Itoa(len(columns)+1)
}

func generateUpdateByFilterQuery(table string, columns []string, filter dbcommon.Filter) (string, []interface{}) {
	setClause := getSetClause(columns)
	query := "UPDATE " + table + " SET " + setClause

	valueNumber := len(columns) + 1
	conditionStr, values := parseCondition(filter.Condition(), &valueNumber)
	if conditionStr != "" {
		query += " WHERE " + conditionStr
	}
	return query, values
}
