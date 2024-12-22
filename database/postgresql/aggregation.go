package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"openauth/database/database/dbcommon"
	"openauth/database/database/dberrors"

	"github.com/gofreego/goutils/logger"
)

func (d *Database) Aggregate(ctx context.Context, record dbcommon.AggregationRecords, filter dbcommon.Aggregator, options ...any) error {
	var rows *sql.Rows
	var err error
	var values []interface{}
	var query string
	prepareName := dbcommon.GetPrepareName(options...)
	if prepareName != "" {
		if d.preparedStatements[prepareName] == nil {
			// prepare the statement
			query, values = generateAggregationQuery(parseTableName(record.Table()), record.AggregationColumns(), filter)
			logger.Debug(ctx, "Database::PostgreSQL::Aggregate::Query:%s: %s", prepareName, query)
			stmt, err := d.conn.PrepareContext(ctx, query)
			if err != nil {
				logger.Error(ctx, "Database::PostgreSQL::Aggregate::Prepare statement failed for name %s, table %s, Err:%s", prepareName, parseTableName(record.Table()), err.Error())
				return dberrors.ParseSQLError("Prepare statement failed for name "+prepareName+", table "+parseTableName(record.Table()), err)
			}
			d.preparedStatements[prepareName] = stmt
		}
		// execute the statement
		rows, err = d.preparedStatements[prepareName].QueryContext(ctx, values...)
	} else {
		query, values = generateAggregationQuery(parseTableName(record.Table()), record.AggregationColumns(), filter)
		logger.Debug(ctx, "Database::PostgreSQL::Aggregate::Query: %s, values: %v", query, values)
		rows, err = d.conn.QueryContext(ctx, query, values...)
	}
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::Aggregate::Aggregate failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("Aggregate failed for table "+parseTableName(record.Table()), err)
	}
	defer rows.Close()
	err = record.ScanRows(rows)
	if err != nil {
		logger.Error(ctx, "Database::PostgreSQL::Aggregate::Scan failed for table %s, Err:%s", parseTableName(record.Table()), err.Error())
		return dberrors.ParseSQLError("Scan failed for table "+parseTableName(record.Table()), err)
	}
	return nil
}

/*

helper function to generate the query for aggregation

*/

func generateAggregationQuery(tableName string, aggregationColumns []*dbcommon.Column, filter dbcommon.Aggregator) (string, []interface{}) {
	query := "SELECT " + parseColumns(aggregationColumns) + " FROM " + tableName
	var values []interface{}
	valueNumber := 1
	if filter == nil {
		return query, values
	}
	condition, condValues := parseCondition(filter.Condition(), &valueNumber)
	if condition != "" {
		query += " WHERE " + condition
		values = append(values, condValues...)
	}

	query += parseGroupBy(filter.GroupBy())
	query += parseSort(filter.Sorts())
	if filter.Offset() > 0 {
		query += fmt.Sprintf(" OFFSET $%d", valueNumber)
		valueNumber++
		values = append(values, filter.Offset())
	}
	if filter.Limit() > 0 {
		query += fmt.Sprintf(" LIMIT $%d", valueNumber)
		valueNumber++
		values = append(values, filter.Limit())
	}
	return query, values
}

func parseColumns(aggregationColumns []*dbcommon.Column) string {
	aggregationColumnsString := ""
	for i, aggregationColumn := range aggregationColumns {
		if i != 0 {
			aggregationColumnsString += ", "
		}
		aggregationColumnsString += parseColumn(aggregationColumn)
	}
	return aggregationColumnsString
}

var aggregationFunctionMap = map[dbcommon.Function]string{
	dbcommon.Count: "COUNT",
	dbcommon.Sum:   "SUM",
	dbcommon.Avg:   "AVG",
	dbcommon.Min:   "MIN",
	dbcommon.Max:   "MAX",
}

func parseColumn(aggregationColumn *dbcommon.Column) string {
	if aggregationColumn.Function == dbcommon.NoFunction {
		return aggregationColumn.Column + parseAlias(aggregationColumn.Alias)
	}
	return fmt.Sprintf("%s(%s)%s", aggregationFunctionMap[aggregationColumn.Function], aggregationColumn.Column, parseAlias(aggregationColumn.Alias))
}

func parseAlias(alias string) string {
	if alias == "" {
		return ""
	}
	return " AS " + alias
}
