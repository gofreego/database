package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	deleteByIDQuery = "DELETE FROM %s WHERE %s = ?"
	deleteQuery     = "DELETE FROM %s WHERE %s"
)

func ParseDeleteByIDQuery(record sql.Record) (string, error) {
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(deleteByIDQuery, tableName, record.IdColumn()), nil
}

func ParseDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error) {
	tableName, err := parseTableName(table)
	if err != nil {
		return "", nil, err
	}
	if condition == nil {
		return fmt.Sprintf(deleteQuery, tableName, "1"), nil, nil // No condition, delete all records
	}
	conditionStr, values, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(deleteQuery, tableName, conditionStr), values, nil
}
