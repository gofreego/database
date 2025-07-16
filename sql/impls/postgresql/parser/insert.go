package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	insertQuery = "INSERT INTO %s (%s) VALUES %s"
)

func ParseInsertQuery(record ...sql.Record) (string, []any, error) {
	if len(record) == 0 {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(record[0].Table())
	if err != nil {
		return "", nil, err
	}
	placeholders, values := getValuesPlaceHolders(record...)
	return fmt.Sprintf(insertQuery, tableName, parseColumns(record[0]), placeholders), values, nil
}

// This function generates PostgreSQL-style placeholders ($1, $2, etc.) for the values in the record.
func getPlaceHolders(count int) string {
	if count <= 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "$" + strconv.Itoa(i+1)
	}
	return strings.Join(placeholders, ", ")
}

// This function parse the columns of the record and returns a string
// representation of the columns, excluding the ID column.
// It is used to create the column list in the SQL INSERT/UPSERT statement.
// For example, if the record has columns ["id", "name", "email"],
// it will return "name, email".
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

// This function generates PostgreSQL-style placeholders for the values in the record.
// it is used to create the VALUES part of the SQL INSERT/UPSERT statement.
func getValuesPlaceHolders(record ...sql.Record) (string, []any) {
	valuesCount := len(record[0].Values())
	valuesPlaceHolders := make([]string, len(record))
	values := make([]any, 0)

	for i := range record {
		placeholders := make([]string, valuesCount)
		for j := 0; j < valuesCount; j++ {
			placeholders[j] = "$" + strconv.Itoa(i*valuesCount+j+1)
		}
		valuesPlaceHolders[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
		values = append(values, record[i].Values()...)
	}
	return strings.Join(valuesPlaceHolders, ", "), values
}
