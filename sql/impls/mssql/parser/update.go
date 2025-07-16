package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mssqlUpdateQuery     = "UPDATE %s SET %s WHERE %s"
	mssqlUpdateByIDQuery = "UPDATE %s SET %s WHERE %s = @p%d"
)

func parseUpdatesMSSQL(updates *sql.Updates) (string, []int, error) {
	var updateClause string
	var valueIndexes []int
	p := 1
	for i, update := range updates.Fields {
		if update.Field == "" {
			return "", nil, errors.New("field is empty for update")
		}
		if update.Value == nil {
			return "", nil, errors.New("value is nil for update field: " + update.Field)
		}
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
			updateClause += fmt.Sprintf("%s = @p%d", update.Field, p)
			valueIndexes = append(valueIndexes, update.Value.Index)
			p++
		}
	}
	return updateClause, valueIndexes, nil
}

func ParseUpdateQuery(table *sql.Table, updates *sql.Updates, condition *sql.Condition) (string, []int, error) {
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
	updateClause, updateValueIndexes, err := parseUpdatesMSSQL(updates)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, updateValueIndexes...)
	conditionQuery, conditionValueIndexes, err := parseCondition(condition)
	if err != nil {
		return "", nil, err
	}
	valueIndexes = append(valueIndexes, conditionValueIndexes...)

	// Convert ? placeholders to @pN format for MSSQL
	query := fmt.Sprintf(mssqlUpdateQuery, tableName, updateClause, conditionQuery)
	p := 1
	for strings.Contains(query, "?") {
		query = strings.Replace(query, "?", fmt.Sprintf("@p%d", p), 1)
		p++
	}

	return query, valueIndexes, nil
}

func ParseUpdateByIDQuery(record sql.Record) (string, error) {
	if record == nil {
		return "", sql.NewInvalidQueryError("update query:: record cannot be nil")
	}
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	updateString := getUpdatesStringMSSQL(record)
	if updateString == "" {
		return "", sql.NewInvalidQueryError("update query:: no columns to update")
	}
	// Calculate the placeholder number for the ID (it's the last parameter)
	idPlaceholder := len(record.Values()) + 1
	return fmt.Sprintf(mssqlUpdateByIDQuery, tableName, updateString, record.IdColumn(), idPlaceholder), nil
}

func getUpdatesStringMSSQL(record sql.Record) string {
	updates := []string{}
	idColumn := record.IdColumn()
	placeholderIndex := 1
	for _, column := range record.Columns() {
		if column == idColumn {
			continue // Skip the ID column in the update
		}
		updates = append(updates, fmt.Sprintf("%s = @p%d", column, placeholderIndex))
		placeholderIndex++
	}
	return strings.Join(updates, ", ")
}
