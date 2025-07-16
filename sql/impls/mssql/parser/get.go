package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mssqlGetByIDQuery = "SELECT %s FROM %s WHERE id = @p1"
	mssqlGetQuery     = "SELECT %s FROM %s"
)

func ParseGetByIDQuery(record sql.Record) (string, error) {
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mssqlGetByIDQuery, strings.Join(record.Columns(), ", "), tableName), nil
}

func ParseGetByFilterQuery(filter *sql.Filter, records sql.Records) (string, []int, error) {
	filterString, values, err := parseFilterMSSQL(filter)
	if err != nil {
		return "", nil, err
	}
	tableName, err := parseTableName(records.Table())
	if err != nil {
		return "", nil, err
	}
	query := fmt.Sprintf(mssqlGetQuery, strings.Join(records.Columns(), ", "), tableName)
	if filterString != "" {
		query += " " + filterString
	}
	return query, values, nil
}

// parseFilterMSSQL should generate MSSQL-style placeholders (@p1, @p2, ...)
// For now, call parseFilter and replace ? with @pN
func parseFilterMSSQL(filter *sql.Filter) (string, []int, error) {
	filterString, valueIndexes, err := parseFilter(filter)
	if err != nil {
		return "", nil, err
	}
	// Replace ? with @pN
	p := 1
	for strings.Contains(filterString, "?") {
		filterString = strings.Replace(filterString, "?", fmt.Sprintf("@p%d", p), 1)
		p++
	}
	return filterString, valueIndexes, nil
}

// parseFilter parses the filter and returns the query string and the values
// returns
// string :: condition string
// []int :: value indexes
// error :: error if any
func parseFilter(filter *sql.Filter) (string, []int, error) {
	if filter == nil {
		return "", nil, nil
	}
	var filterStrings []string
	var filterValues []int
	// condition
	condition, values, err := parseCondition(filter.Condition)
	if err != nil {
		return "", nil, err
	}
	filterStrings = append(filterStrings, fmt.Sprintf("WHERE %s", condition))
	filterValues = append(filterValues, values...)

	// group by
	groupBy := parseGroupBy(filter.GroupBy)
	if groupBy != "" {
		filterStrings = append(filterStrings, groupBy)
	}
	// order by
	orderBy, err := parseOrderBy(filter.Sort)
	if err != nil {
		return "", nil, err
	}
	if orderBy != "" {
		filterStrings = append(filterStrings, orderBy)
	}
	// limit
	if filter.Limit != nil {
		if filter.Limit.Value != nil {
			if v, ok := filter.Limit.Value.(int64); ok && v > 0 {
				filterStrings = append(filterStrings, fmt.Sprintf("LIMIT %d", v))
			} else {
				return "", nil, fmt.Errorf("invalid limit value: %v, expected int/int64 and greater than zero", filter.Limit.Value)
			}
		} else {
			filterStrings = append(filterStrings, "LIMIT ?")
			filterValues = append(filterValues, filter.Limit.Index)
		}
	}
	// offset
	if filter.Offset != nil {
		if filter.Offset.Value != nil {
			if v, ok := filter.Offset.Value.(int64); ok && v >= 0 {
				filterStrings = append(filterStrings, fmt.Sprintf("OFFSET %d", v))
			} else {
				return "", nil, fmt.Errorf("invalid offset value: %v, expected int/int64 and greater than or equal to zero", filter.Offset.Value)
			}
		} else {
			filterStrings = append(filterStrings, "OFFSET ?")
			filterValues = append(filterValues, filter.Offset.Index)
		}
	}

	return strings.Join(filterStrings, " "), filterValues, nil
}

func parseGroupBy(groupBy *sql.GroupBy) string {
	if groupBy == nil {
		return ""
	}
	return fmt.Sprintf("GROUP BY (%s)", strings.Join(groupBy.Fields(), ", "))
}

var orderToStringMap = map[sql.Order]string{
	sql.Asc:  "ASC",
	sql.Desc: "DESC",
}

func parseOrderBy(orderBy *sql.Sort) (string, error) {
	if orderBy == nil {
		return "", nil
	}
	var orderByStrings []string
	var orderStr string
	var ok bool
	for _, field := range orderBy.Fields() {
		if orderStr, ok = orderToStringMap[field.Order]; !ok {
			return "", fmt.Errorf("invalid order: %d for field: %s", field.Order, field.Field)
		}
		orderByStrings = append(orderByStrings, fmt.Sprintf("%s %s", field.Field, orderStr))
	}
	return fmt.Sprintf("ORDER BY %s", strings.Join(orderByStrings, ", ")), nil
}
