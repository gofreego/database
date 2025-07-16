package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	deleteByIDQuery     = "DELETE FROM %s WHERE %s = $1"
	deleteQuery         = "DELETE FROM %s WHERE %s"
	softDeleteByIDQuery = "UPDATE %s SET deleted = 1 WHERE %s = $1"
	softDeleteQuery     = "UPDATE %s SET deleted = 1 WHERE %s"
)

func ParseDeleteByIDQuery(record sql.Record) (string, error) {
	var lastIndex int
	tableName, err := parseTableName(record.Table(), &lastIndex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(deleteByIDQuery, tableName, record.IdColumn()), nil
}

func ParseDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error) {
	var lastIndex int
	tableName, err := parseTableName(table, &lastIndex)
	if err != nil {
		return "", nil, err
	}
	conditionStr, values, err := parseCondition(condition, &lastIndex)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(deleteQuery, tableName, conditionStr), values, nil
}

func ParseSoftDeleteByIDQuery(table *sql.Table, record sql.Record) (string, error) {
	var lastIndex int
	tableName, err := parseTableName(table, &lastIndex)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(softDeleteByIDQuery, tableName, record.IdColumn()), nil
}

func ParseSoftDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error) {
	var lastIndex int
	tableName, err := parseTableName(table, &lastIndex)
	if err != nil {
		return "", nil, err
	}

	conditionStr, values, err := parseCondition(condition, &lastIndex)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(softDeleteQuery, tableName, conditionStr), values, nil
}
