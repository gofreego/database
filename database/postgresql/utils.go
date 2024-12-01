package postgresql

import (
	"database/database/dbcommon"
	"fmt"
	"strconv"
	"strings"
)

var operators = map[dbcommon.Operator]string{}

func parseCondition(c *dbcommon.Condition, valueNumber *int) (string, []interface{}) {
	if c == nil {
		return "", nil
	}
	if c.Operator == dbcommon.OR || c.Operator == dbcommon.AND {
		var conditions []string
		var values []interface{}
		for _, condition := range c.Conditions {
			conditionStr, conditionValues := parseCondition(condition, valueNumber)
			conditions = append(conditions, conditionStr)
			values = append(values, conditionValues...)
		}
		operator := " " + operators[c.Operator] + " "
		return "( " + strings.Join(conditions, operator) + " )", values
	}

	if c.Operator == dbcommon.In || c.Operator == dbcommon.NotIn {
		return fmt.Sprintf("%s %s $%d", c.Column, operators[c.Operator], getValuesMarks(len(c.Values), valueNumber)), c.Values
	}
	*valueNumber++
	return fmt.Sprintf("%s %s $%d", c.Column, operators[c.Operator], *valueNumber-1), []interface{}{c.Value}
}

func getValues(c *dbcommon.Condition) []interface{} {
	if c == nil {
		return nil
	}
	if c.Operator == dbcommon.OR || c.Operator == dbcommon.AND {
		var values []interface{}
		for _, condition := range c.Conditions {
			values = append(values, getValues(condition)...)
		}
		return values
	}
	if c.Operator == dbcommon.In || c.Operator == dbcommon.NotIn {
		return c.Values
	}
	return []interface{}{c.Value}
}

func getValuesMarks(count int, valueNumber *int) string {
	var valueMarks string
	for i := 1; i <= count; i++ {
		valueMarks += "$" + strconv.Itoa(*valueNumber) + ", "
		*valueNumber++
	}
	return "( " + valueMarks[:len(valueMarks)-2] + " )"
}

/*

Sort parsing

*/

var sortOrders = map[dbcommon.Order]string{
	dbcommon.Asc:  "ASC",
	dbcommon.Desc: "DESC",
}

func parseSort(sorts []dbcommon.Sort) string {
	if len(sorts) == 0 {
		return ""
	}
	var sortStr string
	for i, sort := range sorts {
		if i > 0 {
			sortStr += ", "
		}
		sortStr += sort.Column + " " + sortOrders[sort.Order]
	}
	return " ORDER BY " + sortStr
}

/*
GroupBy parsing
*/

func parseGroupBy(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	return " GROUP BY " + strings.Join(columns, ", ")
}
