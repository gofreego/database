package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

// mysql syntax
var (
	joinTypes = map[sql.JoinType]string{
		sql.InnerJoin: "INNER JOIN",
		sql.LeftJoin:  "LEFT JOIN",
		sql.RightJoin: "RIGHT JOIN",
	}
)

func parseTableName(table *sql.Table) (string, error) {
	if table == nil {
		return "", sql.NewInvalidQueryError("invalid table: table cannot be nil")
	}
	joinString, err := parseJoin(table.Join)
	if err != nil {
		return "", nil
	}
	return table.Name + getAlias(table.Alias) + joinString, nil
}

func getAlias(alias string) string {
	if alias == "" {
		return ""
	}
	return " " + alias
}

func parseJoin(join []sql.Join) (string, error) {
	if len(join) == 0 {
		return "", nil
	}
	joins := ""
	for _, j := range join {
		conditionString, _, err := parseCondition(j.On)
		if err != nil {
			return "", err
		}
		tableName, err := parseTableName(j.Table)
		if err != nil {
			return "", err
		}
		joins += fmt.Sprintf(" %s %s ON %s", joinTypes[j.Type], tableName, conditionString)
	}
	return joins, nil
}
