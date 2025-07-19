package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

// This function generates a string of placeholders for the values in the record.
func getPlaceHolders(count int, lastIndex *int) string {
	if count <= 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		*lastIndex++
		placeholders[i] = fmt.Sprintf("$%d", *lastIndex)
	}
	return strings.Join(placeholders, ", ")
}

// This function parse the columns of the record and returns a string
// representation of the columns, excluding the ID column.
// It is used to create the column list in the SQL INSERT/UPSERT statement.
// For example, if the record has columns ["id", "name", "email"],
// it will return "name, email".
func parseInsertColumns(record sql.Record) string {
	columns := []string{}
	idColumn := record.IdColumn()
	for _, col := range record.Columns() {
		if col.Name == idColumn {
			continue
		}
		columns = append(columns, col.Name)
	}
	return strings.Join(columns, ", ")
}

// This function generates a string of placeholders for the values in the record.
// it is used to create the VALUES part of the SQL INSERT/UPSERT statement.
func getValuesPlaceHolders(lastIndex *int, record ...sql.Record) (string, []any) {
	noOfColumns := len(record[0].Values())

	valuesPlaceHolders := make([]string, len(record))
	values := make([]any, 0)
	for i := range len(record) {
		valuesPlaceHolders[i] = fmt.Sprintf("(%s)", getPlaceHolders(noOfColumns, lastIndex))
		values = append(values, record[i].Values()...)
	}
	return strings.Join(valuesPlaceHolders, ", "), values
}

func parseColumns(fields []*sql.Field) string {
	columnStrings := make([]string, len(fields))
	for i, field := range fields {
		columnStrings[i] = parseField(field)
	}
	return strings.Join(columnStrings, ", ")
}

var (
	aggregateFuncMap = map[sql.AggregateFunc]string{
		sql.Count: "COUNT",
		sql.Sum:   "SUM",
		sql.Avg:   "AVG",
		sql.Min:   "MIN",
		sql.Max:   "MAX",
	}
)

func parseField(field *sql.Field) string {
	if field.Name != "" {
		res := field.Name
		if field.Distinct {
			res = "DISTINCT " + res
		}
		if field.Func != sql.None {
			res = fmt.Sprintf("%s(%s)", aggregateFuncMap[field.Func], res)
		}
		if field.Alias != "" {
			res += " AS " + field.Alias
		}
		return res
	}
	if field.Field != nil {
		res := parseField(field.Field)
		if field.Alias != "" {
			res += " AS " + field.Alias
		}
		return res
	}
	return ""
}
