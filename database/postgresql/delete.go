package postgresql

import (
	"context"

	"database/sql"

	"github.com/gofreego/database/database/dbcommon"
	"github.com/gofreego/database/database/dberrors"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) SoftDeleteByID(ctx context.Context, record dbcommon.Record, options ...any) error {
	prepareName := dbcommon.GetPrepareName(options...)
	var result sql.Result
	var err error
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			stmt, err := d.conn.PrepareContext(ctx, generateSoftDeleteByIDQuery(parseTableName(record.Table())))
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::SoftDeleteByID::Prepare statement failed for name %s, table %s, Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		result, err = d.preparedStatements[prepareName].ExecContext(ctx, record.ID())
	} else {
		result, err = d.conn.ExecContext(ctx, generateSoftDeleteByIDQuery(parseTableName(record.Table())), record.ID())
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::SoftDeleteByID::Delete failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("Delete failed for table "+parseTableName(record.Table()), err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::SoftDeleteByID::RowsAffected failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("RowsAffected failed for table "+parseTableName(record.Table()), err)
	}
	if count == 0 {
		return dberrors.NewError(dberrors.ErrRecordNotFound, "Record not found", nil)
	}
	return nil
}

func (d *Database) DeleteByID(ctx context.Context, record dbcommon.Record, options ...any) error {
	prepareName := dbcommon.GetPrepareName(options...)
	var result sql.Result
	var err error
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			stmt, err := d.conn.PrepareContext(ctx, generateDeleteByIDQuery(parseTableName(record.Table())))
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::DeleteByID::Prepare statement failed for name %s, table %s, Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		result, err = d.preparedStatements[prepareName].ExecContext(ctx, record.ID())
	} else {
		result, err = d.conn.ExecContext(ctx, generateDeleteByIDQuery(parseTableName(record.Table())), record.ID())
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::DeleteByID::Delete failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("Delete failed for table "+parseTableName(record.Table()), err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::DeleteByID::RowsAffected failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("RowsAffected failed for table "+parseTableName(record.Table()), err)
	}
	if count == 0 {
		return dberrors.NewError(dberrors.ErrRecordNotFound, "Record not found", nil)
	}
	return nil
}
func (d *Database) DeleteByFilter(ctx context.Context, record dbcommon.Record, filter dbcommon.Filter, options ...any) (int64, error) {
	prepareName := dbcommon.GetPrepareName(options...)
	var result sql.Result
	var err error
	if prepareName != "" {

		query, values := generateDeleteByFilterQuery(parseTableName(record.Table()), filter)
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::DeleteByFilter::Prepare statement failed for name %s, table %s, Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return 0, dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		result, err = d.preparedStatements[prepareName].ExecContext(ctx, values...)
	} else {
		// execute the statement
		query, values := generateDeleteByFilterQuery(parseTableName(record.Table()), filter)
		result, err = d.conn.ExecContext(ctx, query, values...)
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::DeleteByFilter::Delete failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return 0, dberrors.ParseSQLError("Delete failed for table "+parseTableName(record.Table()), err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::DeleteByFilter::RowsAffected failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return 0, dberrors.ParseSQLError("RowsAffected failed for table "+parseTableName(record.Table()), err)
	}
	return count, nil
}

/*
 helper function to generate the delete by id query
*/

func generateDeleteByIDQuery(table string) string {
	return "DELETE FROM " + table + " WHERE id=$1"
}

/*
 helper function to generate the delete by filter query
*/

func generateDeleteByFilterQuery(table string, filter dbcommon.Filter) (string, []interface{}) {
	query := "DELETE FROM " + table
	valueNumber := 1
	conditionStr, values := parseCondition(filter.Condition(), &valueNumber)
	if conditionStr != "" {
		query += " WHERE " + conditionStr
	}
	return query, values
}

/*
 helper function to generate the soft delete by id query
*/

func generateSoftDeleteByIDQuery(table string) string {
	return "UPDATE " + table + " SET deleted = true WHERE id=$1"
}
