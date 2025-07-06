package parser

import (
	"errors"
	"fmt"

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
	placehodlers, values := getValuesPlaceHolders(record...)
	return fmt.Sprintf(insertQuery, tableName, parseColumns(record[0]), placehodlers), values, nil
}
