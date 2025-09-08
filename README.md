# DBmap

`dbmap` is a minimalistic "ORM"/database mapper for Go that provides basic utilities for mapping Go structs to database tables with a focus on ease-of-use.

## Example usage

The primary goal of DBMap is to reduce boilerplate and help developers fall into the "pit of success". For example, all queries run through `dbmap` use named parameters to avoid easy-to-make mistakes with positional parameters.

e.g. `WHERE id = $ID` instead of `WHERE id = ?` + positional args.

```go
conn := sql.Open("sqlite3", ":memory:")
defer conn.Close()
db := dbmap.New(conn)

type User struct {
    ID   int    `db:"id"`
    Name string `db:"name"`

    // Created and Updated at timestamps are automatically updated
    UpdatedAt time.Time `db:"updated_at"`
    CreatedAt time.Time `db:"created_at"`
}

// By default, table names are pluralized struct names (i.e. "users" for User),
// but you can override this by implementing the TableName method.
func (u *User) TableName() string {
    return "my_users"
}

// Select a single record
var user User
// dbmap automatically generates the necessary columns and table name
err := db.Select(ctx, &user, "WHERE id = $ID", dbmap.Args{"ID": 1})

// Select multiple records
var users []User
err = db.Select(ctx, &users, "WHERE name LIKE $pattern", dbmap.Args{"pattern": "A%"})

// Insert a new record
newUser := User{Name: "Alice"}
err = db.Insert(ctx, &newUser)
fmt.Println("New user ID:", newUser.ID) // ID's are automatically populated after inserts

// Update a specific record by ID
user := &User{ID: 1, Name: "Alice"}
err = db.UpdateRecord(ctx, user, dbmap.Updates{"name": "Alicia"})
// The struct is automatically updated in memory
fmt.Println("Updated user name:", user.Name)

// Update arbitrary rows
rowsAffected, err := db.Update(ctx, &User{}, "WHERE name = $name", dbmap.Args{"name": "Alice"}, dbmap.Updates{"name": "Alicia"})
fmt.Println("Updated rows:", rowsAffected)

// Delete a specific record (uses ID)
user := User{ID: 1, Name: "Alice"}
rowsAffected, err = db.DeleteRecord(ctx, &user)

// Delete multiple records (uses ID)
users := []*User{
    {ID: 1, Name: "Alice"},
    {ID: 2, Name: "Bob"},
}
rowsAffected, err = db.DeleteRecords(ctx, users)

// Delete arbitrary records
rowsAffected, err = db.Delete(ctx, &User{}, "WHERE name = $name", dbmap.Args{"name": "Alicia"})
fmt.Println("Deleted rows:", rowsAffected)
```

### Escaping $

Since `dbmap` uses `$` for named parameters, if you need to use a literal `$` in your SQL (e.g. in a string), you can escape it by using `$$`.

## Features (and to-do)

- [x] Support for `insert`ing structs via `DB.InsertRecord`.
- [x] Support for `select`ing structs via `DB.Select`.
- [x] Support for `update`ing data via `DB.Update`.
- [x] Support for `update`ing specific structs via `DB.UpdateRecord`.
- [x] Support for `delete`ing data via `DB.Delete`.
- [x] Support for `delete`ing single structs via `DB.DeleteRecord`.
- [x] Support for `delete`ing multiple structs via `DB.DeleteRecords`.
- [x] Support for transactions via `DB.Transaction`
- [x] Updates `created_at` and `updated_at` fields automatically.
- [x] Support for standard DB `Exec` with named parameters.
- [x] Support for standard DB `Query` with named parameters.
- [x] Pluralize table names by default
- [x] Support for `Exists`
- [x] Support for `Count`

Not in scope, but welcome contributions:

- [ ] Support for non-MySQL databases

Got feature requests or suggestions? Please open an issue or a PR!
