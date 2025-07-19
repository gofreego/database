package common

import (
	"context"
	"testing"

	"github.com/gofreego/database/sql/internal"
)

func TestExecutor_Close(t *testing.T) {
	type fields struct {
		db                 DB
		parser             Parser
		preparedStatements internal.PreparedStatements
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Executor{
				db:                 tt.fields.db,
				parser:             tt.fields.parser,
				preparedStatements: tt.fields.preparedStatements,
			}
			if err := c.Close(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Executor.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
