package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	deleteByIDQuery = "DELETE FROM %s WHERE %s = ?"
)

func ParseDeleteByIDQuery(record sql.Record) (string, error) {
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(deleteByIDQuery, tableName, record.IdColumn()), nil
}
