package parser

import (
	"fmt"
	"strings"

	"github.com/gofreego/database/sql"
)

const (
	mysqlGetByIDQuery = "SELECT %s FROM %s WHERE id = ?"
	mysqlGetQuery     = "SELECT %s FROM %s WHERE %s"
)

func ParseGetByIDQuery(record sql.Record) string {
	return fmt.Sprintf(mysqlGetByIDQuery, strings.Join(record.Columns(), ", "), ParseTable(record.Table()))
}
