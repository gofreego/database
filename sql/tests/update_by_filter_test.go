package tests

import (
	"context"
	"testing"
	"time"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

func TestMysqlDatabase_UpdateByFilter(t *testing.T) {
	tests := []struct {
		name string
		cfg  *sqlfactory.Config
	}{
		{
			name: "mysql update by filter",
			cfg:  &mysqlConfig,
		},
		{
			name: "postgresql update by filter",
			cfg:  &postgresqlConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if err := MigrationUP(ctx, tt.cfg); err != nil {
				t.Errorf("MigrationUP() failed: %v", err)
			}
			defer func() {
				if err := MigrationDown(ctx, tt.cfg); err != nil {
					t.Errorf("MigrationDown() failed: %v", err)
				}
			}()

			db, err := sqlfactory.NewDatabase(ctx, tt.cfg)
			if err != nil {
				t.Errorf("NewDatabase() error = %v", err)
				return
			}
			defer func() {
				if err := db.Close(ctx); err != nil {
					t.Errorf("Close() error = %v", err)
					return
				}
			}()
			if err := db.Ping(ctx); err != nil {
				t.Errorf("Ping() error = %v", err)
				return
			}

			record := &records.User{
				Name:         "John Doe",
				Email:        "john.doe@example.com",
				PasswordHash: "password123",
				IsActive:     1,
				CreatedAt:    time.Now().Unix(),
				UpdatedAt:    time.Now().Unix(),
			}
			if err := db.Insert(ctx, record); err != nil {
				t.Errorf("Insert() error = %v", err)
				return
			}
			/*********
			Update using values
			********/
			// update the record
			updates := sql.NewUpdates().Add("name", sql.NewValue("John Doe Updated"))
			condition := sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewValue(record.ID())}
			noOfRowsUpdated, err := db.Update(ctx, record.Table(), updates, &condition, nil)
			if err != nil {
				t.Errorf("Update() error = %v", err)
				return
			}
			if noOfRowsUpdated != 1 {
				t.Errorf("Update() = %v, want %v", noOfRowsUpdated, 1)
				return
			}
			// get the record
			err = db.GetByID(ctx, record)
			if err != nil {
				t.Errorf("GetByID() error = %v", err)
				return
			}
			if record.Name != "John Doe Updated" {
				t.Errorf("GetByID() = %v, want %v", record.Name, "John Doe Updated")
			}

			/*********
			Update using parameters
			********/
			updates = sql.NewUpdates().Add("name", sql.NewValue("John Doe Updated 2"))
			condition = sql.Condition{Field: "id", Operator: sql.EQ, Value: sql.NewIndexedValue(0)}
			noOfRowsUpdated, err = db.Update(ctx, record.Table(), updates, &condition, []any{record.ID()})
			if err != nil {
				t.Errorf("Update() error = %v", err)
				return
			}
			if noOfRowsUpdated != 1 {
				t.Errorf("Update() = %v, want %v", noOfRowsUpdated, 1)
				return
			}
			// get the record
			err = db.GetByID(ctx, record)
			if err != nil {
				t.Errorf("GetByID() error = %v", err)
				return
			}
			if record.Name != "John Doe Updated 2" {
				t.Errorf("GetByID() = %v, want %v", record.Name, "John Doe Updated 2")
			}

			/*********
			Update multiple records
			********/
			// insert another record
			record2 := &records.User{
				Name:         "John Doe 2",
				Email:        "john.doe2@example.com",
				PasswordHash: "password123",
				IsActive:     1,
				CreatedAt:    time.Now().Unix(),
				UpdatedAt:    time.Now().Unix(),
			}
			if err := db.Insert(ctx, record2); err != nil {
				t.Errorf("Insert() error = %v", err)
				return
			}
			updates = sql.NewUpdates().Add("is_active", sql.NewValue(0))
			condition = sql.Condition{Field: "name", Operator: sql.LIKE, Value: sql.NewIndexedValue(0)}
			noOfRowsUpdated, err = db.Update(ctx, record.Table(), updates, &condition, []any{"%John%"})
			if err != nil {
				t.Errorf("Update() error = %v", err)
				return
			}
			if noOfRowsUpdated != 2 {
				t.Errorf("Update() = %v, want %v", noOfRowsUpdated, 2)
				return
			}
			// get the records
			records := &records.Users{}
			filter := sql.Filter{
				Condition: &sql.Condition{
					Field:    "name",
					Operator: sql.LIKE,
					Value:    sql.NewIndexedValue(0),
				},
			}
			err = db.Get(ctx, &filter, []any{"%John%"}, records)
			if err != nil {
				t.Errorf("Get() error = %v", err)
				return
			}
			if len(records.Users) != 2 {
				t.Errorf("Get() = %v, want %v", len(records.Users), 2)
				return
			}
			if records.Users[0].IsActive != 0 {
				t.Errorf("Get() = %v, want %v", records.Users[0].IsActive, 0)
				return
			}
			if records.Users[1].IsActive != 0 {
				t.Errorf("Get() = %v, want %v", records.Users[1].IsActive, 0)
				return
			}

		})
	}
}
