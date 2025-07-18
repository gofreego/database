package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mysqlUpdateByIDQuery = "UPDATE %s SET %s WHERE %s = ?"
)

func (p *parser) ParseUpdateByIDQuery(record sql.Record) (string, error) {
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
	return fmt.Sprintf(mysqlUpdateByIDQuery, tableName, getUpdatesString(record), record.IdColumn()), nil
}

func getUpdatesString(record sql.Record) string {
	updates := []string{}
	idColumn := record.IdColumn()
	for _, column := range record.Columns() {
		if column == idColumn {
			continue // Skip the ID column in the update
		}
		updates = append(updates, column+" = ?")
	}
	return strings.Join(updates, ", ")
}
