package parser

import (
	"errors"

	"github.com/gofreego/database/sql"
)

// parseFilter parses the filter and returns the query string and the values
// returns
// string :: condition string
// []any :: values
// error :: error if any
func parseFilter(condition *sql.Filter) (string, []any, error) {
	// todo: implement this
	return "", nil, errors.New("not implemented")
}
