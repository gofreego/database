# Database Library

[![Go Version](https://img.shields.io/badge/Go-1.23.3+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/gofreego/database)](https://goreportcard.com/report/github.com/gofreego/database)

A unified, type-safe database interface for Go applications supporting PostgreSQL, MySQL, and MSSQL with a consistent API for common database operations.

## 🚀 Features

- **Multi-Database Support**: PostgreSQL, MySQL, and MSSQL with a single interface
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- **SQL Injection Protection**: Built-in prepared statements and parameterized queries
- **Soft Delete Support**: Mark records as deleted without physical removal
- **Connection Pooling**: Efficient database connection management
- **Transaction Support**: ACID-compliant transaction handling
- **Migration System**: Database schema versioning and management
- **Query Builder**: Flexible filtering, sorting, and joining capabilities
- **Type Safety**: Strongly typed interfaces and error handling

## 📦 Installation

```bash
go get github.com/gofreego/database
```

## 🔧 Quick Start

### 1. Define Your Record Structure

```go
type User struct {
    ID    int64  `db:"id"`
    Name  string `db:"name"`
    Email string `db:"email"`
}

// Implement the Record interface
func (u *User) ID() int64 { return u.ID }
func (u *User) SetID(id int64) { u.ID = id }
func (u *User) IdColumn() string { return "id" }
func (u *User) Table() *sql.Table { return sql.NewTable("users") }
func (u *User) Columns() []string { return []string{"id", "name", "email"} }
func (u *User) Values() []any { return []any{u.Name, u.Email} }
func (u *User) Scan(row sql.Row) error { return row.Scan(&u.ID, &u.Name, &u.Email) }
func (u *User) SetDeleted(deleted bool) { /* implement if needed */ }
```

### 2. Configure Database Connection

```go
import (
    "context"
    "log"
    
    "github.com/gofreego/database/sql"
    "github.com/gofreego/database/sql/sqlfactory"
    "github.com/gofreego/database/sql/impls/postgresql"
)

func main() {
    ctx := context.Background()
    
    // PostgreSQL configuration
    config := &sqlfactory.Config{
        Name: sqlfactory.PostgreSQL,
        PostgreSQL: &postgresql.Config{
            Host:     "localhost",
            Port:     5432,
            User:     "user",
            Password: "password",
            Database: "mydb",
        },
    }
    
    // Create database connection
    db, err := sqlfactory.NewDatabase(ctx, config)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close(ctx)
    
    // Test connection
    if err := db.Ping(ctx); err != nil {
        log.Fatal("Database connection failed:", err)
    }
}
```

### 3. Basic CRUD Operations

```go
// Insert a new user
user := &User{Name: "John Doe", Email: "john@example.com"}
err = db.Insert(ctx, user)
if err != nil {
    log.Printf("Failed to insert user: %v", err)
}

// Get user by ID
user = &User{ID: 1}
err = db.GetByID(ctx, user)
if err != nil {
    if err == sql.ErrNoRecordFound {
        log.Println("User not found")
    } else {
        log.Printf("Failed to get user: %v", err)
    }
}

// Update user
user.Name = "Jane Doe"
updated, err := db.UpdateByID(ctx, user)
if err != nil {
    log.Printf("Failed to update user: %v", err)
}

// Delete user
deleted, err := db.DeleteByID(ctx, user)
if err != nil {
    log.Printf("Failed to delete user: %v", err)
}
```

### 4. Advanced Queries

```go
// Get users with filter
filter := &sql.Filter{
    Condition: &sql.Condition{
        Field:    "email",
        Operator: sql.LIKE,
        Value:    sql.NewValue("%@example.com"),
    },
    Sort: sql.NewSort().Add("name", sql.Asc),
    Limit: sql.NewValue(10),
}

users := &Users{} // Implement Records interface
err = db.Get(ctx, filter, nil, users)

// Update multiple records
updates := sql.NewUpdates().
    Add("is_active", sql.NewValue(false))

condition := &sql.Condition{
    Field:    "last_login",
    Operator: sql.LT,
    Value:    sql.NewValue(time.Now().AddDate(0, -6, 0)), // 6 months ago
}

affected, err := db.Update(ctx, sql.NewTable("users"), updates, condition, nil)
```

## 🗄️ Supported Databases

### PostgreSQL
```go
config := &sqlfactory.Config{
    Name: sqlfactory.PostgreSQL,
    PostgreSQL: &postgresql.Config{
        Host:     "localhost",
        Port:     5432,
        User:     "user",
        Password: "password",
        Database: "mydb",
        SSLMode:  "disable",
    },
}
```

### MySQL
```go
config := &sqlfactory.Config{
    Name: sqlfactory.MySQL,
    MySQL: &mysql.Config{
        Host:     "localhost",
        Port:     3306,
        User:     "user",
        Password: "password",
        Database: "mydb",
    },
}
```

### MSSQL
```go
config := &sqlfactory.Config{
    Name: sqlfactory.MSSQL,
    MSSQL: &mssql.Config{
        Host:     "localhost",
        Port:     1433,
        User:     "sa",
        Password: "password",
        Database: "mydb",
    },
}
```

## 🔄 Migrations

The library includes a migration system for database schema management:

```go
import "github.com/gofreego/database/sql/migrator"

// Run migrations
migrator := migrator.New(db)
err = migrator.Up(ctx)

// Rollback migrations
err = migrator.Down(ctx, 1) // Rollback 1 migration
```

## 🧪 Testing

The library includes comprehensive test suites for all supported databases:

```bash
# Start test databases
make setup-db-up

# Run tests with coverage
make test

# View coverage report
make view-coverage

# Stop test databases
make setup-db-down
```

## 📚 API Reference

### Core Interfaces

- **Database**: Main interface for database operations
- **Record**: Interface for single database records
- **Records**: Interface for collections of records
- **Row/Rows**: Interfaces for query result iteration

### Query Building

- **Filter**: Complex query filtering with conditions, sorting, and pagination
- **Condition**: Individual query conditions with operators
- **Table**: Table definitions with join support
- **Updates**: Field update specifications

### Operators

- **Comparison**: `EQ`, `NEQ`, `GT`, `GTE`, `LT`, `LTE`
- **Pattern Matching**: `LIKE`, `NOTLIKE`
- **Set Operations**: `IN`, `NOTIN`
- **Logical**: `AND`, `OR`

## 🔒 Security Features

- **Prepared Statements**: Automatic SQL injection prevention
- **Parameterized Queries**: Safe value binding
- **Input Validation**: Type checking and sanitization
- **Connection Security**: SSL/TLS support for all databases

## 🚀 Performance

- **Connection Pooling**: Efficient connection reuse
- **Prepared Statement Caching**: Query optimization
- **Batch Operations**: Bulk insert/update support
- **Lazy Loading**: On-demand data fetching

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [API Reference](https://pkg.go.dev/github.com/gofreego/database)
- **Issues**: [GitHub Issues](https://github.com/gofreego/database/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gofreego/database/discussions)

## 🙏 Acknowledgments

- Built with Go's standard `database/sql` package
- Inspired by modern ORM patterns
- Community-driven development
