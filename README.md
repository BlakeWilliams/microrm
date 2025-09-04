# micrORM

`microrm` is a minimalistic "ORM" for Go that provides basic utilities for mapping Go structs to database tables with a focus on ease-of-use.

## Example usage

While `microrm` is designed as a minimal ORM, its primary goal is to reduce boilerplate and help developers fall into the "put of success". For example, all queries run through `microrm` use named parameters to avoid easy-to-make mistakes with positional parameters.

e.g. `SELECT * FROM users WHERE id = $ID` instead of `SELECT * FROM users WHERE id = ?`.

```go
conn := sql.Open("sqlite3", ":memory:")
defer conn.Close()
db := microrm.New(conn)
db.MapNameToTable("User", "users") // map struct name to table name

type User struct {
    ID   int    `db:"id"`
    Name string `db:"name"`
}

// Select some data
var user User
// micorm automatically generates the necessary columns and table name
_ := db.Get(&user, "WHERE id = $ID", map[string]any{"ID": 1})

newUser := User{Name: "Alice"}
// microrm automatically generates the necessary columns and table name
_ := db.Insert(&newUser)
fmt.Println("New user ID:", newUser.ID) // ID's are automatically populated after inserts
```

### Escaping $

Since `microrm` uses `$` for named parameters, if you need to use a literal `$` in your SQL (e.g. in a string), you can escape it by using `$$`.

## Features (and to-do)

- [x] Support for `select`ing data via `DB.Select`.
- [x] Support for `insert`ing data via `DB.Insert`.
- [ ] Support for `update`ing data via `DB.Update`.
- [x] Support for `delete`ing data via `DB.Delete`.
- [x] Support for `delete`ing specific structs via `DB.DeleteRecord`.
- [x] Support for transactions via `DB.Transaction`
- [ ] Updates `created_at` and `updated_at` fields automatically.

Not in scope, but welcome contributions:

- [ ] Support for non-MySQL databases

Got feature requests or suggestions? Please open an issue or a PR!
