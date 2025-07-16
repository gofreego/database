package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mysqlGetByIDQuery = "SELECT %s FROM %s WHERE id = ?"
	mysqlGetQuery     = "SELECT %s FROM %s"
)

func ParseGetByIDQuery(record sql.Record) (string, error) {
	var lastIndex int
	tableName, err := parseTableName(record.Table(), &lastIndex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mysqlGetByIDQuery, strings.Join(record.Columns(), ", "), tableName), nil
}

func ParseGetByFilterQuery(filter *sql.Filter, records sql.Records) (string, []int, error) {
	var lastIndex int
	tableName, err := parseTableName(records.Table(), &lastIndex)
	if err != nil {
		return "", nil, err
	}
	filterString, values, err := parseFilter(filter, &lastIndex)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf(mysqlGetQuery, strings.Join(records.Columns(), ", "), tableName)
	if filterString != "" {
		query += " " + filterString
	}
	return query, values, nil
}
