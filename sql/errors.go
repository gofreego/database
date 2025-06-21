package sql

import "errors"

var (
	ErrInvalidConfig    = errors.New("invalid config")
	ErrNoRecordFound    = errors.New("no record found")
	ErrNoRecordInserted = errors.New("no record inserted")
)
