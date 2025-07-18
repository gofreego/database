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
			want:    "UPDATE users SET name = $1, email = $2, password_hash = $3, is_active = $4, created_at = $5, updated_at = $6 WHERE id = $7",
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
