package dberrors

import (
	"database/sql"
	"fmt"
)

type Code int

const (
	ErrUnknown Code = iota
	ErrConnectionClosed
	ErrInvalidQuery
	ErrRecordNotFound
	ErrInvalidConfig
	ErrTxnClosed
)

type Error struct {
	code    Code
	message string
	err     error
}

func (e *Error) Code() Code {
	return e.code
}

func (e *Error) Unwrap() error {
	return e.err
}

func NewError(errType Code, message string, err error) error {
	return &Error{
		code:    errType,
		message: message,
		err:     err,
	}
}

func (e *Error) Error() string {
	if e.err == nil {
		return fmt.Sprintf("Database Error:: %s:: Code: %d", e.message, e.code)
	} else {
		return fmt.Sprintf("Database Error:: %s:: Code: %d, Err: %s", e.message, e.code, e.err.Error())
	}
}

func IsRecordNotFound(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.code == ErrRecordNotFound
	}
	return false
}

func ParseSQLError(message string, err error) error {
	switch err {
	case sql.ErrNoRows:
		return NewError(ErrRecordNotFound, message, err)
	case sql.ErrTxDone:
		return NewError(ErrTxnClosed, message, err)
	case sql.ErrConnDone:
		return NewError(ErrConnectionClosed, message, err)
	}
	return NewError(ErrInvalidQuery, message, err)
}
