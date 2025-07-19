package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	upsertQuery = "INSERT INTO %s (%s) VALUES %s ON CONFLICT (id) DO UPDATE SET %s"
)

func (p *parser) ParseUpsertQuery(record sql.Record) (string, []any, error) {
	if record == nil {
		return "", nil, errors.New("no record provided")
	}
	var lastIndex int
	tableName, err := parseTableName(record.Table(), &lastIndex)
	if err != nil {
		return "", nil, err
	}
	placeholders, values := getValuesPlaceHolders(&lastIndex, record)
	if len(values) == 0 {
		return "", nil, errors.New("no values provided for upsert")
	}

	updates := parseUpsertUpdates(record)
	if updates == "" {
		return "", nil, errors.New("no columns to update")
	}

	return fmt.Sprintf(upsertQuery, tableName, parseInsertColumns(record), placeholders, updates), values, nil
}

func parseUpsertUpdates(record sql.Record) string {
	updates := []string{}
	idColumn := record.IdColumn()
	for _, col := range record.Columns() {
		if col.Name == idColumn {
			continue // Skip the ID column in the update
		}
		updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col.Name, col.Name))
	}
	return strings.Join(updates, ", ")
}
