package tests

import (
	"context"
	"testing"

	"github.com/gofreego/database/sql"
	"github.com/gofreego/database/sql/sqlfactory"
	"github.com/gofreego/database/sql/tests/records"
)

var (
	aggregationTestData = []sql.Record{
		&records.User{
			Id:           1,
			Name:         "John Doe",
			Email:        "john.doe@example.com",
			PasswordHash: "password",
			Score:        100,
			IsActive:     1,
			CreatedAt:    1716153600,
			UpdatedAt:    1716153600,
		},
		&records.User{
			Id:           2,
			Name:         "Jane Doe",
			Email:        "jane.doe@example.com",
			PasswordHash: "password",
			Score:        200,
			IsActive:     1,
			CreatedAt:    1716153600,
			UpdatedAt:    1716153600,
		},
		&records.User{
			Id:           3,
			Name:         "John Smith",
			Email:        "john.smith@example.com",
			PasswordHash: "password",
			Score:        300,
			IsActive:     1,
			CreatedAt:    1716153600,
			UpdatedAt:    1716153600,
		},
		&records.User{
			Id:           4,
			Name:         "Jane Smith",
			Email:        "jane.smith@example.com",
			PasswordHash: "password",
			Score:        300,
		},
	}
)

// setupTestDatabase sets up the test environment
func setupAggregationTestDatabase(t *testing.T, config *sqlfactory.Config) (sql.Database, func()) {
	ctx := context.Background()

	// Run migrations
	MigrationUP(ctx, config, t)

	// Create database connection
	db, err := sqlfactory.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("failed to create database connection: %v", err)
	}

	// Test connection
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("failed to ping database: %v", err)
	}

	// Insert test data
	if err := generateTestData(db, aggregationTestData); err != nil {
		t.Fatalf("failed to generate test data: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		db.Close(ctx)
		MigrationDown(ctx, config, t)
	}

	return db, cleanup
}

type result struct {
	Value []int64 `sql:"value"`
	records.User
}

func (u *result) Columns() []*sql.Field {
	return []*sql.Field{
		sql.DistinctOf(sql.NewField("score")).As("value"),
	}
}
func (u *result) Scan(rows sql.Rows) error {
	u.Value = make([]int64, 0)
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}
func TestDistinct(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &result{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 3 {
				t.Fatalf("expected 3 users, got %d", len(res.Value))
			}
		})
	}
}

type resultCount struct {
	Value []int64 `sql:"value"`
	records.User
}

func (u *resultCount) Columns() []*sql.Field {
	return []*sql.Field{
		sql.CountOf(sql.NewField("score")).As("value"),
	}
}
func (u *resultCount) Scan(rows sql.Rows) error {
	u.Value = make([]int64, 0)
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}
func TestCount(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultCount{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 4 {
				t.Fatalf("expected 4 users, got %d", res.Value[0])
			}
		})
	}
}

type resultSum struct {
	Value []int64 `sql:"value"`
	records.User
}

func (u *resultSum) Columns() []*sql.Field {
	return []*sql.Field{
		sql.SumOf(sql.NewField("score")).As("value"),
	}
}
func (u *resultSum) Scan(rows sql.Rows) error {
	u.Value = make([]int64, 0)
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}

func TestSum(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultSum{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 900 {
				t.Fatalf("expected 900, got %d", res.Value[0])
			}
		})
	}
}

type resultAvg struct {
	Value []float64 `sql:"value"`
	records.User
}

func (u *resultAvg) Columns() []*sql.Field {
	return []*sql.Field{
		sql.AvgOf(sql.NewField("score")).As("value"),
	}
}
func (u *resultAvg) Scan(rows sql.Rows) error {
	u.Value = make([]float64, 0)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}

func TestAvg(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultAvg{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 225 {
				t.Fatalf("expected 225, got %f", res.Value[0])
			}
		})
	}
}

type resultMin struct {
	Value []float64 `sql:"value"`
	records.User
}

func (u *resultMin) Columns() []*sql.Field {
	return []*sql.Field{
		sql.MinOf(sql.NewField("score")).As("value"),
	}
}
func (u *resultMin) Scan(rows sql.Rows) error {
	u.Value = make([]float64, 0)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}

func TestMin(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultMin{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 100 {
				t.Fatalf("expected 100, got %f", res.Value[0])
			}
		})
	}
}

type resultMax struct {
	Value []float64 `sql:"value"`
	records.User
}

func (u *resultMax) Columns() []*sql.Field {
	return []*sql.Field{
		sql.MaxOf(sql.NewField("score")).As("value"),
	}
}
func (u *resultMax) Scan(rows sql.Rows) error {
	u.Value = make([]float64, 0)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}

func TestMax(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultMax{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 300 {
				t.Fatalf("expected 300, got %f", res.Value[0])
			}
		})
	}
}

type resultCountDistinct struct {
	Value []float64 `sql:"value"`
	records.User
}

func (u *resultCountDistinct) Columns() []*sql.Field {
	return []*sql.Field{
		sql.CountOf(sql.DistinctOf(sql.NewField("score"))).As("value"),
	}
}
func (u *resultCountDistinct) Scan(rows sql.Rows) error {
	u.Value = make([]float64, 0)
	for rows.Next() {
		var value float64
		if err := rows.Scan(&value); err != nil {
			return err
		}
		u.Value = append(u.Value, value)
	}
	return nil
}

func TestCountDistinct(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, cleanup := setupAggregationTestDatabase(t, tt.args.config)
			defer cleanup()
			ctx := tt.args.ctx
			res := &resultCountDistinct{}
			err := db.Get(ctx, &sql.Filter{}, nil, res)
			if err != nil {
				t.Fatalf("failed to get users: %v", err)
			}
			if len(res.Value) != 1 {
				t.Fatalf("expected 1 user, got %d", len(res.Value))
			}
			if res.Value[0] != 3 {
				t.Fatalf("expected 3, got %f", res.Value[0])
			}
		})
	}
}
