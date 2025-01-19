package postgresql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gofreego/database/database/dbcommon"
)

var operators = map[dbcommon.Operator]string{
	dbcommon.EQ:    " = ",
	dbcommon.NEQ:   " != ",
	dbcommon.LT:    " < ",
	dbcommon.LTE:   " <= ",
	dbcommon.GT:    " > ",
	dbcommon.GTE:   " >= ",
	dbcommon.Like:  " LIKE ",
	dbcommon.In:    " IN ",
	dbcommon.NotIn: " NOT IN ",
	dbcommon.OR:    " OR ",
	dbcommon.AND:   " AND ",
}

func parseValue(value interface{}) interface{} {
	switch v := value.(type) {
	case *dbcommon.Column:
		return parseColumn(value.(*dbcommon.Column))
	case bool:
		return strconv.FormatBool(v)
	default:
		return value
	}
}

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
		return fmt.Sprintf("%s %s %s", c.Column, operators[c.Operator], getValuesMarks(len(c.Values), valueNumber)), c.Values
	}
	switch c.Value.(type) {
	case *dbcommon.Column:
		return fmt.Sprintf("%s %s %s", c.Column, operators[c.Operator], parseColumn(c.Value.(*dbcommon.Column))), nil
	default:
		*valueNumber++
		return fmt.Sprintf("%s %s $%d", c.Column, operators[c.Operator], *valueNumber-1), []interface{}{c.Value}
	}

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

/*

Parse Table

*/

var joinTypeToPGSQL = map[dbcommon.JoinType]string{
	dbcommon.InnerJoin: "INNER JOIN",
	dbcommon.LeftJoin:  "LEFT JOIN",
	dbcommon.RightJoin: "RIGHT JOIN",
	dbcommon.FullJoin:  "FULL JOIN",
}

func tableString(table *dbcommon.Table) string {
	if table.Alias != "" {
		return table.Name + " AS " + table.Alias
	}
	return table.Name
}

func parseJoin(join *dbcommon.Join) string {
	condStr, _ := parseCondition(join.On, new(int))
	return joinTypeToPGSQL[join.JoinType] + tableString(join.Table) + " ON " + condStr
}

func parseTableName(table *dbcommon.Table) string {
	str := tableString(table) + " "
	for _, join := range table.Joins {
		str += parseJoin(join) + " "
	}
	return str
}
