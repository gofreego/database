package records

import "github.com/gofreego/database/sql"

type User struct {
	Id           int64  `sql:"id"`
	Name         string `sql:"name"`
	Email        string `sql:"email"`
	PasswordHash string `sql:"password_hash"`
	IsActive     int    `sql:"is_active"`
	CreatedAt    int64  `sql:"created_at"`
	UpdatedAt    int64  `sql:"updated_at"`
}

func (u *User) IdColumn() string {
	return "id"
}

// Columns implements sql.Record.
func (u *User) Columns() []string {
	return []string{"id", "name", "email", "password_hash", "is_active", "created_at", "updated_at"}
}

// ID implements sql.Record.
func (u *User) ID() int64 {
	return u.Id
}

// Scan implements sql.Record.
func (u *User) Scan(row sql.Row) error {
	return row.Scan(&u.Id, &u.Name, &u.Email, &u.PasswordHash, &u.IsActive, &u.CreatedAt, &u.UpdatedAt)
}

// SetID implements sql.Record.
func (u *User) SetID(id int64) {
	u.Id = id
}

// Table implements sql.Record.
func (u *User) Table() *sql.Table {
	return &sql.Table{
		Name: "users",
	}
}

// Values implements sql.Record.
func (u *User) Values() []any {
	return []any{u.Name, u.Email, u.PasswordHash, u.IsActive, u.CreatedAt, u.UpdatedAt}
}

type Users struct {
	User
	Users []*User
}

func (u *Users) ScanMany(rows sql.Rows) error {
	u.Users = make([]*User, 0)
	for rows.Next() {
		user := new(User)
		if err := user.Scan(rows); err != nil {
			return err
		}
		u.Users = append(u.Users, user)
	}
	return nil
}
