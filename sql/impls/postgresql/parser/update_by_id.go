package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	postgresqlUpdateByIDQuery = "UPDATE %s SET %s WHERE %s = $%d"
)

func ParseUpdateByIDQuery(record sql.Record) (string, error) {
	if record == nil {
		return "", sql.NewInvalidQueryError("update query:: record cannot be nil")
	}
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", err
	}
	updateString := getUpdatesString(record)
	if updateString == "" {
		return "", sql.NewInvalidQueryError("update query:: no columns to update")
	}
	// Calculate the placeholder number for the ID (it's the last parameter)
	idPlaceholder := len(record.Values()) + 1
	return fmt.Sprintf(postgresqlUpdateByIDQuery, tableName, getUpdatesString(record), record.IdColumn(), idPlaceholder), nil
}

func getUpdatesString(record sql.Record) string {
	updates := []string{}
	idColumn := record.IdColumn()
	placeholderIndex := 1
	for _, column := range record.Columns() {
		if column == idColumn {
			continue // Skip the ID column in the update
		}
		updates = append(updates, fmt.Sprintf("%s = $%d", column, placeholderIndex))
		placeholderIndex++
	}
	return strings.Join(updates, ", ")
}
