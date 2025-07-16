package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mssqlUpdateByIDQuery = "UPDATE %s SET %s WHERE %s = @p%d"
)

func ParseUpdateByIDQuery(record sql.Record) (string, error) {
	if record == nil {
		return "", sql.NewInvalidQueryError("update query:: record cannot be nil")
	}
	var lastIndex int
	tableName, err := parseTableName(record.Table(), &lastIndex)
	if err != nil {
		return "", err
	}
	updateString := getUpdatesString(record, &lastIndex)
	if updateString == "" {
		return "", sql.NewInvalidQueryError("update query:: no columns to update")
	}
	lastIndex++
	return fmt.Sprintf(mssqlUpdateByIDQuery, tableName, updateString, record.IdColumn(), lastIndex), nil
}

func getUpdatesString(record sql.Record, lastIndex *int) string {
	updates := []string{}
	idColumn := record.IdColumn()
	for _, column := range record.Columns() {
		if column == idColumn {
			continue // Skip the ID column in the update
		}
		*lastIndex++
		updates = append(updates, fmt.Sprintf("%s = @p%d", column, *lastIndex))
	}
	return strings.Join(updates, ", ")
}
