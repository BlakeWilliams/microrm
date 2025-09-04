# microrm

`microrm` is a minimalistic "ORM" for Go that provides basic utilities for mapping Go structs to database tables with a focus on ease-of-use.

## Example usage

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
```

## Features (and to-do)

- [x] Support for `select`ing data via `DB.Select`.
- [ ] Support for `insert`ing data via `DB.Insert`.
- [ ] Support for `update`ing data via `DB.Update`.
- [ ] Support for `delete`ing data via `DB.Delete`.

Got feature requests or suggestions? Please open an issue or a PR!
