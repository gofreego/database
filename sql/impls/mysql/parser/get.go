package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	mysqlGetByIDQuery = "SELECT %s FROM %s WHERE id = ?"
	mysqlGetQuery     = "SELECT %s FROM %s"
)

func (p *parser) ParseGetByIDQuery(record sql.Record) (string, error) {
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mysqlGetByIDQuery, parseColumns(record.Columns()), tableName), nil
}

func (p *parser) ParseGetByFilterQuery(filter *sql.Filter, records sql.Records) (string, []int, error) {
	filterString, values, err := parseFilter(filter)
	if err != nil {
		return "", nil, err
	}
	tableName, err := parseTableName(records.Table())
	if err != nil {
		return "", nil, err
	}
	query := fmt.Sprintf(mysqlGetQuery, parseColumns(records.Columns()), tableName)
	if filterString != "" {
		query += " " + filterString
	}
	return query, values, nil
}
