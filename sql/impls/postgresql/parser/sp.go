package parser

import "fmt"

const (
	// postgresql sp query format
	SPQueryFormat = "CALL %s(%s)"
)

func (p *parser) ParseSPQuery(spName string, values []any) (string, error) {
	var index int
	return fmt.Sprintf(SPQueryFormat, spName, getPlaceHolders(len(values), &index)), nil
}
