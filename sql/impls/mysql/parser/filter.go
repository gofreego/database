package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

// parseFilter parses the filter and returns the query string and the values
// returns
// string :: condition string
// []any :: values
// error :: error if any
func parseFilter(filter *sql.Filter) (string, []*sql.Value, error) {
	if filter == nil {
		return "", nil, nil
	}
	var filterStrings []string
	var filterValues []*sql.Value
	// condition
	condition, values, err := parseCondition(filter.Condition)
	if err != nil {
		return "", nil, err
	}
	if condition != "" {
		filterStrings = append(filterStrings, fmt.Sprintf("WHERE %s", condition))
		filterValues = append(filterValues, values...)
	}
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
			filterValues = append(filterValues, filter.Limit.WithType(sql.Int))
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
			filterValues = append(filterValues, filter.Offset.WithType(sql.Int))
		}
	}

	return strings.Join(filterStrings, " "), filterValues, nil
}

func parseGroupBy(groupBy *sql.GroupBy) string {
	if groupBy == nil {
		return ""
	}
	return "GROUP BY " + strings.Join(groupBy.Fields(), ", ")
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
	return "ORDER BY " + strings.Join(orderByStrings, ", "), nil
}
