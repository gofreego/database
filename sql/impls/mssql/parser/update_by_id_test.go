package parser

import (
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseUpdateByIDQuery(t *testing.T) {
	type args struct {
		record sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "valid record",
			args: args{
				record: &records.User{Id: 1, Name: "Alice", Email: "alice@example.com", PasswordHash: "hash123", IsActive: 1, CreatedAt: 123456789, UpdatedAt: 987654321},
			},
			want:    "UPDATE users SET name = @p1, email = @p2, password_hash = @p3, score = @p4, is_active = @p5, created_at = @p6, updated_at = @p7 WHERE id = @p8",
			wantErr: false,
		},
		{
			name: "nil record",
			args: args{
				record: nil,
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "missing table",
			args: args{
				record: &mockNoTableRecord{},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prsr.ParseUpdateByIDQuery(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpdateByIDQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseUpdateByIDQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseUpdateByIDQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		record  sql.Record
		wantErr bool
	}{
		{
			name:    "nil record",
			record:  nil,
			wantErr: true,
		},
		{
			name:    "record with nil table",
			record:  &mockNoTableRecord{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := prsr.ParseUpdateByIDQuery(tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpdateByIDQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
