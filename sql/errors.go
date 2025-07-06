package sql

import (
	"fmt"
)

type ErrorCode int

const (
	ErrCodeInvalidConfig ErrorCode = iota + 1
	ErrCodeNoRecordFound
	ErrCodeNoRecordInserted
	ErrCodeInvalidQuery
)

type Error struct {
	message string
	code    ErrorCode
}

func (e *Error) Error() string {
	return fmt.Sprintf("sql error: code : %d, err: %s", e.code, e.message)
}

func (e *Error) Code() ErrorCode {
	return e.code
}

func (e *Error) IsQueryError() bool {
	return e.code == ErrCodeInvalidQuery
}

var (
	ErrInvalidConfig    = &Error{message: "invalid config", code: ErrCodeInvalidConfig}
	ErrNoRecordFound    = &Error{message: "no record found", code: ErrCodeNoRecordFound}
	ErrNoRecordInserted = &Error{message: "no record inserted", code: ErrCodeNoRecordInserted}
)

func NewInvalidQueryError(message string) error {
	return &Error{
		message: message,
		code:    ErrCodeInvalidQuery,
	}
}
