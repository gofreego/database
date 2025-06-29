package parser

import (
	"errors"

	"github.com/gofreego/database/sql"
)

/*
parseCondition parses the condition and returns the query string and the values
returns
string :: condition string
[]any :: values
*/
func parseCondition(condition *sql.Condition) (string, []any, error) {
	return "", nil, errors.New("not implemented")
}
