package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

/*
This file contains tests for the get functionality of the sql package.
This will cover all the different ways that a get query can be constructed.
*/

var (
	users []sql.Record = []sql.Record{
		&records.User{
			Id:           1,
			Name:         "John Doe",
			Email:        "john.doe@example.com",
			PasswordHash: "bcrypt_hash_123",
			IsActive:     1,
		},
		&records.User{
			Id:           2,
			Name:         "Jane Smith",
			Email:        "jane.smith@example.com",
			PasswordHash: "sha256_hash_456",
			IsActive:     1,
		},
		&records.User{
			Id:           3,
			Name:         "Michael Johnson",
			Email:        "michael.johnson@example.com",
			PasswordHash: "bcrypt_hash_789",
			IsActive:     0, // Inactive user
		},
		&records.User{
			Id:           4,
			Name:         "Emily Davis",
			Email:        "emily.davis@example.com",
			PasswordHash: "sha256_hash_abc",
			IsActive:     1,
		},
		&records.User{
			Id:           5,
			Name:         "David Wilson",
			Email:        "david.wilson@example.com",
			PasswordHash: "bcrypt_hash_def",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641340800000, // 2022-01-05 00:00:00
			UpdatedAt:    1641427200000, // 2022-01-06 00:00:00
		},
		&records.User{
			Id:           6,
			Name:         "Sarah Brown",
			Email:        "sarah.brown@example.com",
			PasswordHash: "sha256_hash_ghi",
			IsActive:     1,
			CreatedAt:    1641427200000, // 2022-01-06 00:00:00
			UpdatedAt:    1641427200000,
		},
		&records.User{
			Id:           7,
			Name:         "Robert Taylor",
			Email:        "robert.taylor@example.com",
			PasswordHash: "bcrypt_hash_jkl",
			IsActive:     1,
		},
		&records.User{
			Id:           8,
			Name:         "Lisa Anderson",
			Email:        "lisa.anderson@example.com",
			PasswordHash: "sha256_hash_mno",
			IsActive:     0,             // Inactive user
			CreatedAt:    1641600000000, // 2022-01-08 00:00:00
			UpdatedAt:    1641686400000, // 2022-01-09 00:00:00
		},
		&records.User{
			Id:           9,
			Name:         "James Martinez",
			Email:        "james.martinez@example.com",
			PasswordHash: "bcrypt_hash_pqr",
			IsActive:     1,
			CreatedAt:    1641686400000, // 2022-01-09 00:00:00
			UpdatedAt:    1641686400000,
		},
		&records.User{
			Id:           10,
			Name:         "Jennifer Garcia",
			Email:        "jennifer.garcia@example.com",
			PasswordHash: "sha256_hash_stu",
			IsActive:     1,
		},
		&records.User{
			Id:           11,
			Name:         "Christopher Rodriguez",
			Email:        "christopher.rodriguez@example.com",
			PasswordHash: "bcrypt_hash_vwx",
			IsActive:     0, // Inactive user

		},
		&records.User{
			Id:           12,
			Name:         "Amanda Lewis",
			Email:        "amanda.lewis@example.com",
			PasswordHash: "sha256_hash_yz1",
			IsActive:     1,
		},
		&records.User{
			Id:           13,
			Name:         "Daniel Lee",
			Email:        "daniel.lee@example.com",
			PasswordHash: "bcrypt_hash_234",
			IsActive:     1,
		},
		&records.User{
			Id:           14,
			Name:         "Michelle White",
			Email:        "michelle.white@example.com",
			PasswordHash: "sha256_hash_567",
			IsActive:     0, // Inactive user

		},
		&records.User{
			Id:           15,
			Name:         "Kevin Harris",
			Email:        "kevin.harris@example.com",
			PasswordHash: "bcrypt_hash_890",
			IsActive:     1,
		},
		&records.User{
			Id:           16,
			Name:         "Nicole Clark",
			Email:        "nicole.clark@example.com",
			PasswordHash: "sha256_hash_abc",
			IsActive:     1,
		},
		&records.User{
			Id:           17,
			Name:         "Steven Hall",
			Email:        "steven.hall@example.com",
			PasswordHash: "bcrypt_hash_def",
			IsActive:     0, // Inactive user

		},
		&records.User{
			Id:           18,
			Name:         "Rachel Young",
			Email:        "rachel.young@example.com",
			PasswordHash: "sha256_hash_ghi",
			IsActive:     1,
		},
		&records.User{
			Id:           19,
			Name:         "Thomas King",
			Email:        "thomas.king@example.com",
			PasswordHash: "bcrypt_hash_jkl",
			IsActive:     1,
		},
		&records.User{
			Id:           20,
			Name:         "Jessica Wright",
			Email:        "jessica.wright@example.com",
			PasswordHash: "sha256_hash_mno",
			IsActive:     0, // Inactive user

		},
	}
)

func generateTestData(db sql.Database) error {

	noOfInserted, err := db.InsertMany(context.Background(), users, sql.Options{})
	if err != nil {
		return err
	}

	fmt.Println("noOfInserted", noOfInserted)

	if noOfInserted != int64(len(users)) {
		return fmt.Errorf("expected %d records to be inserted, got %d", len(users), noOfInserted)
	}
	return nil
}

func TestGetByFilterEQ(t *testing.T) {
	tests := []testCase{
		{
			name: "mysql get by filter",
			args: args{
				ctx:    context.Background(),
				config: &mysqlConfig,
			},
		},
		{
			name: "postgresql get by filter",
			args: args{
				ctx:    context.Background(),
				config: &postgresqlConfig,
			},
		},
		{
			name: "mssql insert and get",
			args: args{
				ctx:    context.Background(),
				config: &mssqlConfig,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// migration up
			MigrationUP(context.Background(), tt.args.config, t)
			// migration down
			defer MigrationDown(context.Background(), tt.args.config, t)
			// connect to database
			db, err := sqlfactory.NewDatabase(context.Background(), tt.args.config)
			if err != nil {
				t.Errorf("sqlfactory.NewDatabase() failed: %v", err)
			}
			defer db.Close(context.Background())
			// generate test data and insert into database
			if err := generateTestData(db); err != nil {
				t.Errorf("generateTestData() failed: %v", err)
			}
			// get by filter

		})
	}
}
