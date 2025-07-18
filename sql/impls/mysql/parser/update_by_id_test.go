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
			name: "normal user record",
			args: args{
				record: &records.User{
					Id:           1,
					Name:         "Test",
					Email:        "test@example.com",
					PasswordHash: "hash",
					IsActive:     1,
					CreatedAt:    123,
					UpdatedAt:    456,
				},
			},
			want:    "UPDATE users SET name = ?, email = ?, password_hash = ?, is_active = ?, created_at = ?, updated_at = ? WHERE id = ?",
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
		{
			name: "only id column",
			args: args{
				record: &mockIdOnlyRecord{Id: 1},
			},
			want:    "",
			wantErr: true, // Will generate an empty SET clause, which is not valid SQL but matches the function output
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
