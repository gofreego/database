package parser

import (
	"errors"
	"fmt"

	"github.com/gofreego/database/sql"
)

const (
	updateQuery = "UPDATE %s SET %s WHERE %s"
)

func (p *parser) ParseUpdateQuery(table *sql.Table, updates *sql.Updates, condition *sql.Condition) (string, []int, error) {
	var valueIndexes []int
	var updateClause string
	var err error

	if updates == nil {
		return "", nil, errors.New("updates is nil")
	}
	tableName, err := parseTableName(table)
	if err != nil {
		return "", nil, err
	}
	updateClause, updateValueIndexes, err := parseUpdates(updates)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, updateValueIndexes...)
	conditionQuery, conditionValueIndexes, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, conditionValueIndexes...)

	return fmt.Sprintf(updateQuery, tableName, updateClause, conditionQuery), valueIndexes, nil
}

func parseUpdates(updates *sql.Updates) (string, []int, error) {
	var updateClause string
	var valueIndexes []int

	for i, update := range updates.Fields {
		if update.Field == "" {
			return "", nil, errors.New("field is empty for update")
		}
		if update.Value == nil {
			return "", nil, errors.New("value is nil for update field: " + update.Field)
		}

		// Add comma separator if not the first field
		if i > 0 {
			updateClause += ", "
		}

		if update.Value.IsColumn() {
			if update.Value.Value == nil || update.Value.Value == "" {
				return "", nil, errors.New("value is empty for update field: " + update.Field + " and value type is a column")
			}
			updateClause += fmt.Sprintf("%s = %s", update.Field, update.Value.Value)
		} else if update.Value.Value != nil {
			updateClause += fmt.Sprintf("%s = %s", update.Field, getValue(update.Value.Value))
		} else {
			updateClause += fmt.Sprintf("%s = ?", update.Field)
			valueIndexes = append(valueIndexes, update.Value.Index)
		}
	}

	return updateClause, valueIndexes, nil
}
