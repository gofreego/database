package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	postgresqlUpdateByIDQuery = "UPDATE %s SET %s WHERE %s = $%d"
)

func (p *parser) ParseUpdateByIDQuery(record sql.Record) (string, error) {
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
	return fmt.Sprintf(postgresqlUpdateByIDQuery, tableName, updateString, record.IdColumn(), lastIndex), nil
}

func getUpdatesString(record sql.Record, lastIndex *int) string {
	updates := []string{}
	idColumn := record.IdColumn()
	for _, column := range record.Columns() {
		if column == idColumn {
			continue // Skip the ID column in the update
		}
		*lastIndex++
		updates = append(updates, fmt.Sprintf("%s = $%d", column, *lastIndex))
	}
	return strings.Join(updates, ", ")
}
