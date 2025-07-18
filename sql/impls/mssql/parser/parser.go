package parser

import "github.com/gofreego/database/sql/impls/common"

var (
	prsr = &parser{}
)

type parser struct {
}

func NewParser() common.Parser {
	return &parser{}
}
