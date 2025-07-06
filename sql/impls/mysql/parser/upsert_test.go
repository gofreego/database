package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseUpsertQuery(t *testing.T) {
	type args struct {
		record sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []any
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
			want:    "INSERT INTO users (name, email, password_hash, is_active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email), password_hash = VALUES(password_hash), is_active = VALUES(is_active), created_at = VALUES(created_at), updated_at = VALUES(updated_at)",
			want1:   []any{"Test", "test@example.com", "hash", 1, int64(123), int64(456)},
			wantErr: false,
		},
		{
			name: "nil record",
			args: args{
				record: nil,
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "missing table",
			args: args{
				record: &mockNoTableRecord{},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name: "only id column",
			args: args{
				record: &mockIdOnlyRecord{Id: 1},
			},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseUpsertQuery(tt.args.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseUpsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseUpsertQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseUpsertQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
