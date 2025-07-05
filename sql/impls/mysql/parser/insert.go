package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	insertQuery = "INSERT INTO %s (%s) VALUES %s"
	upsertQuery = "INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s"
)

func ParseInsertQuery(record ...sql.Record) (string, []any, error) {
	if len(record) == 0 {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(record[0].Table())
	if err != nil {
		return "", nil, err
	}
	placehodlers, values := getValuesPlaceHolders(record...)
	return fmt.Sprintf(insertQuery, tableName, parseColumns(record[0]), placehodlers), values, nil
}

func ParseUpsertQuery(records ...sql.Record) (string, []any, error) {
	if len(records) == 0 {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(records[0].Table())
	if err != nil {
		return "", nil, err
	}
	placehodlers, values := getValuesPlaceHolders(records...)
	return fmt.Sprintf(upsertQuery, tableName, parseColumns(records[0]), placehodlers, parseUpdates(records[0])), values, nil
}

/*
Helper functions
*/

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

func getValuesPlaceHolders(record ...sql.Record) (string, []any) {
	placeholder := getPlaceHolders(len(record[0].Values()))
	valuesPlaceHolders := make([]string, len(record))
	values := make([]any, 0)
	for i := range record {
		valuesPlaceHolders[i] = fmt.Sprintf("(%s)", placeholder)
		values = append(values, record[i].Values()...)
	}
	return strings.Join(valuesPlaceHolders, ", "), values
}

func parseUpdates(record sql.Record) string {
	updates := []string{}

	for _, col := range record.Columns() {
		updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", col, col))
	}
	return strings.Join(updates, ", ")
}
