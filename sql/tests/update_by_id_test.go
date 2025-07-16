package tests

import (
	"context"
	"testing"
	"time"

	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

func TestMysqlDatabase_UpdateByID(t *testing.T) {
	tests := []struct {
		name string
		cfg  *sqlfactory.Config
	}{
		{
			name: "mysql update by id",
			cfg:  &mysqlConfig,
		},
		{
			name: "postgresql update by id",
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
				}
			}()
			if err := db.Ping(ctx); err != nil {
				t.Errorf("Ping() error = %v", err)
				return
			}
			// insert a record
			record := &records.User{
				Id:           1,
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
			// update the record
			record.Name = "Jane Doe Updated"
			record.Email = "jane.doe@example.updated"
			record.PasswordHash = "password1234"
			record.IsActive = 0
			got, err := db.UpdateByID(ctx, record)
			if err != nil {
				t.Errorf("UpdateByID() error = %v", err)
				return
			}
			if !got {
				t.Errorf("UpdateByID() = %v, want %v", got, true)
			}
			// get the record
			err = db.GetByID(ctx, record)
			if err != nil {
				t.Errorf("GetByID() error = %v", err)
				return
			}
			if record.Name != "Jane Doe Updated" {
				t.Errorf("GetByID() = %v, want %v", record.Name, "Jane Doe Updated")
			}
			if record.Email != "jane.doe@example.updated" {
				t.Errorf("GetByID() = %v, want %v", record.Email, "jane.doe@example.updated")
			}
			if record.PasswordHash != "password1234" {
				t.Errorf("GetByID() = %v, want %v", record.PasswordHash, "password1234")
			}
			if record.IsActive != 0 {
				t.Errorf("GetByID() = %v, want %v", record.IsActive, 0)
			}
		})
	}
}
