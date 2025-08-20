package parser

import "fmt"

const (
	// mysql sp query format
	SPQueryFormat = "CALL %s(%s)"
)

func (p *parser) ParseSPQuery(spName string, values []any) (string, error) {
	return fmt.Sprintf(SPQueryFormat, spName, getPlaceHolders(len(values))), nil
}
