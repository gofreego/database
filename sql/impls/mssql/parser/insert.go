package parser

import (
	"errors"
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	insertQuery = "INSERT INTO %s (%s) VALUES %s"
)

func (p *parser) ParseInsertQuery(record ...sql.Record) (string, []any, error) {
	if len(record) == 0 {
		return "", nil, errors.New("no record provided")
	}
	var lastIndex int
	tableName, err := parseTableName(record[0].Table(), &lastIndex)
	if err != nil {
		return "", nil, err
	}

	placehodlers, values := getValuesPlaceHolders(&lastIndex, record...)
	return fmt.Sprintf(insertQuery, tableName, parseColumns(record[0]), placehodlers), values, nil
}
