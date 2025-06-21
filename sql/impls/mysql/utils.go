package mysql

import (
	db "database/sql"

	"github.com/gofreego/database/sql"
)

func handleError(err error) error {
	if err == db.ErrNoRows {
		return sql.ErrNoRecordFound
	}
	return err
}
