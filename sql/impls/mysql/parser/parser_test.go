package parser

import (
	"testing"

	"github.com/gofreego/database/sql/impls/common"
)

func TestNewParser(t *testing.T) {
	tests := []struct {
		name string
		want common.Parser
	}{
		{
			name: "create new parser",
			want: &parser{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewParser()
			if got == nil {
				t.Errorf("NewParser() returned nil")
				return
			}

			// Check if it implements the Parser interface
			if _, ok := got.(common.Parser); !ok {
				t.Errorf("NewParser() does not implement common.Parser interface")
			}

			// Check if it's the correct type
			if _, ok := got.(*parser); !ok {
				t.Errorf("NewParser() returned wrong type, expected *parser")
			}
		})
	}
}
