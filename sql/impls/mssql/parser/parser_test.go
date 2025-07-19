package parser

import (
	"testing"

	"github.com/gofreego/database/sql/impls/common"
)

func TestNewParser(t *testing.T) {
	parser := NewParser()

	// Check that the parser is not nil
	if parser == nil {
		t.Error("NewParser() returned nil")
	}

	// Check that it implements the common.Parser interface
	var _ common.Parser = parser
}
