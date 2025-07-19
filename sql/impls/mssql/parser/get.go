package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	mssqlGetByIDQuery = "SELECT %s FROM %s WHERE id = @p1"
	mssqlGetQuery     = "SELECT %s FROM %s"
)

func (p *parser) ParseGetByIDQuery(record sql.Record) (string, error) {
	var lastIndex int
	tableName, err := parseTableName(record.Table(), &lastIndex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mssqlGetByIDQuery, parseColumns(record.Columns()), tableName), nil
}

func (p *parser) ParseGetByFilterQuery(filter *sql.Filter, records sql.Records) (string, []int, error) {
	var lastIndex int
	tableName, err := parseTableName(records.Table(), &lastIndex)
	if err != nil {
		return "", nil, err
	}
	filterString, values, err := parseFilter(filter, &lastIndex)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf(mssqlGetQuery, parseColumns(records.Columns()), tableName)
	if filterString != "" {
		query += " " + filterString
	}
	return query, values, nil
}
