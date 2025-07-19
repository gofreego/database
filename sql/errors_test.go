package sql

import (
	"errors"
	"testing"
)

func TestNewDatabaseError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantCode ErrorCode
	}{
		{
			name: "nil error",
			args: args{
				err: nil,
			},
			wantErr:  false,
			wantCode: 0,
		},
		{
			name: "simple error",
			args: args{
				err: errors.New("database connection failed"),
			},
			wantErr:  true,
			wantCode: ErrUnknownDatabaseError,
		},
		{
			name: "empty error message",
			args: args{
				err: errors.New(""),
			},
			wantErr:  true,
			wantCode: ErrUnknownDatabaseError,
		},
		{
			name: "complex error message",
			args: args{
				err: errors.New("connection timeout: dial tcp 127.0.0.1:5432: connect: connection refused"),
			},
			wantErr:  true,
			wantCode: ErrUnknownDatabaseError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewDatabaseError(tt.args.err)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDatabaseError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				sqlErr, ok := err.(*Error)
				if !ok {
					t.Errorf("NewDatabaseError() returned error is not of type *Error")
					return
				}

				if sqlErr.Code() != tt.wantCode {
					t.Errorf("NewDatabaseError() error code = %v, want %v", sqlErr.Code(), tt.wantCode)
				}

				if sqlErr.Error() == "" {
					t.Errorf("NewDatabaseError() error message is empty")
				}
			}
		})
	}
}

func TestNewInvalidQueryError(t *testing.T) {
	type args struct {
		message string
		args    []any
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantCode    ErrorCode
		wantMessage string
	}{
		{
			name: "simple message",
			args: args{
				message: "invalid query syntax",
				args:    []any{},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "invalid query syntax",
		},
		{
			name: "message with format arguments",
			args: args{
				message: "query failed for table %s with error: %s",
				args:    []any{"users", "syntax error"},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "query failed for table users with error: syntax error",
		},
		{
			name: "empty message",
			args: args{
				message: "",
				args:    []any{},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "",
		},
		{
			name: "message with multiple format arguments",
			args: args{
				message: "failed to execute query on %s.%s: %s",
				args:    []any{"database", "table", "permission denied"},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "failed to execute query on database.table: permission denied",
		},
		{
			name: "message with numeric arguments",
			args: args{
				message: "query timeout after %d seconds, retry count: %d",
				args:    []any{30, 3},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "query timeout after 30 seconds, retry count: 3",
		},
		{
			name: "message with mixed type arguments",
			args: args{
				message: "table %s has %d columns, but query expects %s",
				args:    []any{"users", 5, "6 columns"},
			},
			wantErr:     true,
			wantCode:    ErrCodeInvalidQuery,
			wantMessage: "table users has 5 columns, but query expects 6 columns",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewInvalidQueryError(tt.args.message, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewInvalidQueryError() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				sqlErr, ok := err.(*Error)
				if !ok {
					t.Errorf("NewInvalidQueryError() returned error is not of type *Error")
					return
				}

				if sqlErr.Code() != tt.wantCode {
					t.Errorf("NewInvalidQueryError() error code = %v, want %v", sqlErr.Code(), tt.wantCode)
				}

				if sqlErr.Error() == "" {
					t.Errorf("NewInvalidQueryError() error message is empty")
				}

				// Check if the error message contains the expected content
				if tt.wantMessage != "" && sqlErr.Error() != "sql error: code : 4, err: "+tt.wantMessage {
					t.Errorf("NewInvalidQueryError() error message = %v, want to contain %v", sqlErr.Error(), tt.wantMessage)
				}
			}
		})
	}
}

func TestError_IsQueryError(t *testing.T) {
	tests := []struct {
		name string
		err  *Error
		want bool
	}{
		{
			name: "query error",
			err:  &Error{code: ErrCodeInvalidQuery},
			want: true,
		},
		{
			name: "not query error",
			err:  &Error{code: ErrCodeNoRecordFound},
			want: false,
		},
		{
			name: "database error",
			err:  &Error{code: ErrUnknownDatabaseError},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.IsQueryError(); got != tt.want {
				t.Errorf("Error.IsQueryError() = %v, want %v", got, tt.want)
			}
		})
	}
}
