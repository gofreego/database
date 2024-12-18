package postgresql

import (
	"context"
	"fmt"
	"openauth/database/database/dbcommon"
)

func (d *Database) Aggregate(ctx context.Context, record dbcommon.AggregationRecords, filter dbcommon.Filter, options ...any) error {
	return nil
}

/*

helper function to generate the query for aggregation

*/

func generateAggregationQuery(tableName string, aggregationColumns []dbcommon.AggregationColumn, filter dbcommon.Filter) (string, []interface{}) {
	query := "SELECT " + parseAggregationColumns(aggregationColumns) + " FROM " + tableName
	var values []interface{}
	valueNumber := 1
	if filter != nil {

		condition, condValues := parseCondition(filter.Condition(), &valueNumber)
		if condition != "" {
			query += " WHERE " + condition
			values = append(values, condValues...)
		}
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

func parseAggregationColumns(aggregationColumns []dbcommon.AggregationColumn) string {
	aggregationColumnsString := ""
	for i, aggregationColumn := range aggregationColumns {
		if i != 0 {
			aggregationColumnsString += ", "
		}
		aggregationColumnsString += parseAggregationColumn(aggregationColumn)
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

func parseAggregationColumn(aggregationColumn dbcommon.AggregationColumn) string {
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
