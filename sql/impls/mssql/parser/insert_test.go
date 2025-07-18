package parser

import (
	"reflect"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/tests/records"
)

func TestParseInsertQuery(t *testing.T) {
	type args struct {
		record []sql.Record
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []any
		wantErr bool
	}{
		{
			name:    "no records (error)",
			args:    args{record: nil},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
		{
			name:    "single record, single column (id only)",
			args:    args{record: []sql.Record{&mockIdOnlyRecord{Id: 1}}},
			want:    "INSERT INTO mock () VALUES ()",
			want1:   []any{},
			wantErr: false,
		},
		{
			name:    "multiple records, single column (id only batch)",
			args:    args{record: []sql.Record{&mockIdOnlyRecord{Id: 1}, &mockIdOnlyRecord{Id: 2}}},
			want:    "INSERT INTO mock () VALUES (), ()",
			want1:   []any{},
			wantErr: false,
		},
		{
			name: "single record, multi column (user)",
			args: args{record: []sql.Record{&records.User{
				Id: 1, Name: "Alice", Email: "alice@example.com", PasswordHash: "hash123",
				IsActive: 1, CreatedAt: 123456789, UpdatedAt: 987654321,
			}}},
			want:    "INSERT INTO users (name, email, password_hash, is_active, created_at, updated_at) VALUES (@p1, @p2, @p3, @p4, @p5, @p6)",
			want1:   []any{"Alice", "alice@example.com", "hash123", 1, int64(123456789), int64(987654321)},
			wantErr: false,
		},
		{
			name: "multiple records, multi column (user batch)",
			args: args{record: []sql.Record{
				&records.User{Id: 1, Name: "Alice", Email: "alice@example.com", PasswordHash: "hash123", IsActive: 1, CreatedAt: 123456789, UpdatedAt: 987654321},
				&records.User{Id: 2, Name: "Bob", Email: "bob@example.com", PasswordHash: "hash456", IsActive: 0, CreatedAt: 123456790, UpdatedAt: 987654322},
			}},
			want:    "INSERT INTO users (name, email, password_hash, is_active, created_at, updated_at) VALUES (@p1, @p2, @p3, @p4, @p5, @p6), (@p7, @p8, @p9, @p10, @p11, @p12)",
			want1:   []any{"Alice", "alice@example.com", "hash123", 1, int64(123456789), int64(987654321), "Bob", "bob@example.com", "hash456", 0, int64(123456790), int64(987654322)},
			wantErr: false,
		},
		{
			name:    "record with nil table (error)",
			args:    args{record: []sql.Record{&mockNoTableRecord{}}},
			want:    "",
			want1:   nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := prsr.ParseInsertQuery(tt.args.record...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseInsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ParseInsertQuery() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseInsertQuery() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
