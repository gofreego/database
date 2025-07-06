package internal

import (
	db "database/sql"

	"github.com/gofreego/database/sql"
)

func HandleError(err error) error {
	if err == nil {
		return nil
	}
	if err == db.ErrNoRows {
		return sql.ErrNoRecordFound
	}
	return sql.NewDatabaseError(err)
}
