package parser

import (
	"errors"
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	updateQuery = "UPDATE %s SET %s WHERE %s"
)

func parseUpdates(updates *sql.Updates) (string, []int, error) {
	var updateQuery string
	var valueIndexes []int

	for _, update := range updates.Fields {
		if update.Field == "" {
			return "", nil, errors.New("field is empty for update")
		}
		if update.Value == nil {
			return "", nil, errors.New("value is nil for update field: " + update.Field)
		}

		if update.Value.IsColumn() {
			if update.Value.Value == nil || update.Value.Value == "" {
				return "", nil, errors.New("value is empty for update field: " + update.Field + " and value type is a column")
			}
			updateQuery += fmt.Sprintf("%s = %s", update.Field, update.Value.Value)
		} else if update.Value.Value != nil {
			updateQuery += fmt.Sprintf("%s = %s", update.Field, getValue(update.Value.Value))
		} else {
			updateQuery += fmt.Sprintf("%s = ?", update.Field)
			valueIndexes = append(valueIndexes, update.Value.Index)
		}
	}

	return updateQuery, valueIndexes, nil
}

func ParseUpdateQuery(table *sql.Table, updates *sql.Updates, condition *sql.Condition) (string, []int, error) {
	var valueIndexes []int
	var updateQuery string
	var err error

	if updates == nil {
		return "", nil, errors.New("updates is nil")
	}
	tableName, err := parseTableName(table)
	if err != nil {
		return "", nil, err
	}
	conditionQuery, conditionValueIndexes, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, conditionValueIndexes...)
	updateQuery, updateValueIndexes, err := parseUpdates(updates)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, updateValueIndexes...)

	return fmt.Sprintf(updateQuery, tableName, updateQuery, conditionQuery), valueIndexes, nil
}
