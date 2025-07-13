package internal

import (
	"database/sql"
	"slices"
)

// This will be used internally
type PreparedStatement struct {
	Statement *sql.Stmt
	// This is getting used in case of batch insert, update, delete
	noOfRecords        int
	valueIndexes       []int
	noOfValuesRequired int
}

func NewPreparedStatement(s *sql.Stmt) *PreparedStatement {
	return &PreparedStatement{
		Statement: s,
	}
}

func (s *PreparedStatement) WithRecords(n int) *PreparedStatement {
	s.noOfRecords = n
	return s
}

func (s *PreparedStatement) GetNoOfRecords() int {
	return s.noOfRecords
}

func (s *PreparedStatement) GetStatement() *sql.Stmt {
	return s.Statement
}

func (s *PreparedStatement) WithValueIndexes(valueIndexes []int) *PreparedStatement {
	s.valueIndexes = valueIndexes
	s.noOfValuesRequired = slices.Max(valueIndexes) + 1
	return s
}

func (s *PreparedStatement) GetValueIndexes() []int {
	return s.valueIndexes
}

func (s *PreparedStatement) GetNoOfValuesRequired() int {
	return s.noOfValuesRequired
}

/*
*
*
*
*
* Data structure for prepared statements
*
*
*
*
 */
type PreparedStatements map[string]*PreparedStatement

func NewPreparedStatements() PreparedStatements {
	stmts := make(PreparedStatements, 0)
	return stmts
}

func (ss PreparedStatements) Add(key string, s *PreparedStatement) {
	ss[key] = s
}

func (ss PreparedStatements) Get(key string) (*PreparedStatement, bool) {
	s, f := ss[key]
	return s, f
}

func (ss PreparedStatements) Exists(key string) bool {
	return ss[key] != nil
}

func (ss PreparedStatements) Close() error {
	for _, stmt := range ss {
		err := stmt.Statement.Close()
		if err != nil {
			return HandleError(err)
		}
	}
	return nil
}
