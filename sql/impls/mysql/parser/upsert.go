package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	upsertQuery = "INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s"
)

func (p *parser) ParseUpsertQuery(record sql.Record) (string, []any, error) {
	if record == nil {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", nil, err
	}
	placeholders, values := getValuesPlaceHolders(record)
	if len(values) == 0 {
		return "", nil, errors.New("no values provided for upsert")
	}

	updates := parseUpsertUpdates(record)
	if updates == "" {
		return "", nil, errors.New("no columns to update")
	}

	return fmt.Sprintf(upsertQuery, tableName, parseColumns(record), placeholders, updates), values, nil
}

func parseUpsertUpdates(record sql.Record) string {
	updates := []string{}
	idColumn := record.IdColumn()
	for _, col := range record.Columns() {
		if col == idColumn {
			continue // Skip the ID column in the update
		}
		updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", col, col))
	}
	return strings.Join(updates, ", ")
}
