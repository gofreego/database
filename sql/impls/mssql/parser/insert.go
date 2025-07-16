package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mssqlInsertQuery = "INSERT INTO %s (%s) VALUES %s"
)

func ParseInsertQuery(record ...sql.Record) (string, []any, error) {
	if len(record) == 0 {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(record[0].Table())
	if err != nil {
		return "", nil, err
	}
	placeholders, values := getValuesPlaceHoldersMSSQL(record...)
	return fmt.Sprintf(mssqlInsertQuery, tableName, parseColumns(record[0]), placeholders), values, nil
}

// getValuesPlaceHoldersMSSQL returns MSSQL-style placeholders and values
func getValuesPlaceHoldersMSSQL(records ...sql.Record) (string, []any) {
	var placeholders string
	var values []any
	p := 1
	for i, record := range records {
		if i > 0 {
			placeholders += ", "
		}
		placeholders += "("
		for j, v := range record.Values() {
			if j > 0 {
				placeholders += ", "
			}
			placeholders += fmt.Sprintf("@p%d", p)
			values = append(values, v)
			p++
		}
		placeholders += ")"
	}
	return placeholders, values
}

// parseTableName adapted from MySQL parser
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
		joins += fmt.Sprintf(" INNER JOIN %s ON %s", tableName, conditionString) // Use INNER JOIN for now
	}
	return joins, nil
}

// parseColumns adapted from MySQL parser/utils.go
func parseColumns(record sql.Record) string {
	columns := []string{}
	idColumn := record.IdColumn()
	for _, col := range record.Columns() {
		if col == idColumn {
			continue
		}
		columns = append(columns, col)
	}
	return strings.Join(columns, ", ")
}
