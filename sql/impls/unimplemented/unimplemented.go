package unimplemented

import (
	"context"
	"errors"

	"github.com/gofreego/database/sql"
)

type Unimplemented struct {
}

func (u *Unimplemented) Ping(ctx context.Context) error {
	return errors.New("Ping method is not implemented")
}

func (u *Unimplemented) Close(ctx context.Context) error {
	return errors.New("Close method is not implemented")
}

func (u *Unimplemented) Insert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	return errors.New("Insert method is not implemented")
}

func (u *Unimplemented) InsertMany(ctx context.Context, records []sql.Record, options ...sql.Options) (int64, error) {
	return 0, errors.New("InsertMany method is not implemented")
}

func (u *Unimplemented) Upsert(ctx context.Context, record sql.Record, options ...sql.Options) error {
	return errors.New("Upsert method is not implemented")
}

func (u *Unimplemented) GetByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
	return errors.New("GetByID method is not implemented")
}

func (u *Unimplemented) Get(ctx context.Context, filter *sql.Filter, values []any, records sql.Records, options ...sql.Options) error {
	return errors.New("GetByFilter method is not implemented")
}

func (u *Unimplemented) UpdateByID(ctx context.Context, record sql.Record, options ...sql.Options) error {
	return errors.New("UpdateByID method is not implemented")
}

func (u *Unimplemented) Update(ctx context.Context, updates *sql.Updates, condition *sql.Condition, values []any, options ...sql.Options) error {
	return errors.New("UpdateByCondition method is not implemented")
}

func (u *Unimplemented) DeleteByID(ctx context.Context, id int64, options ...sql.Options) error {
	return errors.New("DeleteByID method is not implemented")
}

func (u *Unimplemented) Delete(ctx context.Context, condition *sql.Condition, values []any, options ...sql.Options) error {
	return errors.New("DeleteByCondition method is not implemented")
}

func (u *Unimplemented) SoftDelete(ctx context.Context, id int64, options ...sql.Options) error {
	return errors.New("SoftDeleteByID method is not implemented")
}
