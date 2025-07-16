package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	mssqlDeleteByIDQuery     = "DELETE FROM %s WHERE %s = @p1"
	mssqlDeleteQuery         = "DELETE FROM %s WHERE %s"
	mssqlSoftDeleteByIDQuery = "UPDATE %s SET deleted = 1 WHERE %s = @p1"
	mssqlSoftDeleteQuery     = "UPDATE %s SET deleted = 1 WHERE %s"
)

func ParseDeleteByIDQuery(record sql.Record) (string, error) {
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mssqlDeleteByIDQuery, tableName, record.IdColumn()), nil
}

func ParseDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error) {
	tableName, err := parseTableName(table)
	if err != nil {
		return "", nil, err
	}
	conditionStr, values, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(mssqlDeleteQuery, tableName, conditionStr), values, nil
}

func ParseSoftDeleteByIDQuery(table *sql.Table, record sql.Record) (string, error) {
	tableName, err := parseTableName(table)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(mssqlSoftDeleteByIDQuery, tableName, record.IdColumn()), nil
}

func ParseSoftDeleteQuery(table *sql.Table, condition *sql.Condition) (string, []int, error) {
	tableName, err := parseTableName(table)
	if err != nil {
		return "", nil, err
	}
	conditionStr, values, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf(mssqlSoftDeleteQuery, tableName, conditionStr), values, nil
}
