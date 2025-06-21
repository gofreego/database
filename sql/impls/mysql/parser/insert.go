package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	insertQuery = "INSERT INTO %s (%s) VALUES %s"
)

func ParseInsertQuery(record ...sql.Record) string {
	if len(record) == 0 {
		return ""
	}
	return fmt.Sprintf(insertQuery, ParseTable(record[0].Table()), strings.Join(record[0].Columns(), ", "), getValuesPlaceHolders(record...))
}

func getValuesPlaceHolders(record ...sql.Record) string {
	placeholder := getPlaceHolders(len(record[0].Columns()))
	valuesPlaceHolders := make([]string, len(record))
	for i := range record {
		valuesPlaceHolders[i] = fmt.Sprintf("(%s)", placeholder)
	}
	return strings.Join(valuesPlaceHolders, ", ")
}
