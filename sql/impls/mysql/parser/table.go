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

func ParseTable(table *sql.Table) string {

	return table.Name + getAlias(table.Alias) + parseJoin(table.Join)
}

func getAlias(alias string) string {
	if alias == "" {
		return ""
	}
	return " " + alias
}

func parseJoin(join []sql.Join) string {
	if len(join) == 0 {
		return ""
	}
	joins := ""
	for _, j := range join {
		joins += fmt.Sprintf(" %s %s ON %s", joinTypes[j.Type], ParseTable(j.Table), parseCondition(j.On))
	}
	return joins
}
