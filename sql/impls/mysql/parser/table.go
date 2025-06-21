package parser

import (
	"fmt"

	"github.com/gofreego/database/sql"
)

func ParseTable(table *sql.Table) string {
	return fmt.Sprintf("SELECT * FROM %s", table.Name)
}
