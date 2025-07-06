package internal

import "database/sql"

// This will be used internally
type PreparedStatement struct {
	Statement   *sql.Stmt
	NoOfRecords int
}

func NewPreparedStatement(s *sql.Stmt) *PreparedStatement {
	return &PreparedStatement{
		Statement: s,
	}
}

func (s *PreparedStatement) Records(n int) *PreparedStatement {
	s.NoOfRecords = n
	return s
}

func (s *PreparedStatement) GetStatement() *sql.Stmt {
	return s.Statement
}

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
