package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	insertQuery  = "INSERT INTO %s (%s) VALUES %s"
	upsertQuery  = "INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s"
	updateSyntax = "%s = VALUES(%s)"
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

func ParseUpsertQuery(records ...sql.Record) string {
	if len(records) == 0 {
		return ""
	}
	return fmt.Sprintf(upsertQuery, ParseTable(records[0].Table()), strings.Join(records[0].Columns(), ", "), getValuesPlaceHolders(records...), parseUpdates(records[0]))
}

func parseUpdates(record sql.Record) string {
	updates := []string{}

	for _, col := range record.Columns() {
		updates = append(updates, fmt.Sprintf(updateSyntax, col, col))
	}
	return strings.Join(updates, ", ")
}
