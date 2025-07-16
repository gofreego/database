package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

// ParseUpsertQuery generates a MERGE statement for MSSQL upsert
func ParseUpsertQuery(record sql.Record) (string, []any, error) {
	if record == nil {
		return "", nil, errors.New("no record provided")
	}
	tableName, err := parseTableName(record.Table())
	if err != nil {
		return "", nil, err
	}
	columns := []string{}
	values := []any{}
	placeholders := []string{}
	idColumn := record.IdColumn()
	idValue := record.ID()
	updateAssignments := []string{}
	p := 1
	for _, col := range record.Columns() {
		if col == idColumn {
			continue
		}
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("@p%d", p))
		values = append(values, record.Values()[p-1])
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s = source.%s", col, col))
		p++
	}
	// Add id value for matching
	values = append(values, idValue)
	merge := fmt.Sprintf(`MERGE INTO %s AS target USING (SELECT %s) AS source (%s) ON (target.%s = @p%d)
WHEN MATCHED THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
		tableName,
		strings.Join(placeholders, ", "),
		strings.Join(columns, ", "),
		idColumn, p,
		strings.Join(updateAssignments, ", "),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))
	return merge, values, nil
}
