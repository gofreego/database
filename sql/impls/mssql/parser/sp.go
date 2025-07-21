package parser

import (
	"fmt"
)

const (
	// mssql sp query format
	SPQueryFormat = "EXEC %s %s"
)

func (p *parser) ParseSPQuery(spName string, values []any) (string, error) {
	var index int
	query := fmt.Sprintf(SPQueryFormat, spName, getPlaceHolders(len(values), &index))
	return query, nil
}
