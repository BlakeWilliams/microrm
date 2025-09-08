package dbmap_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/blakewilliams/dbmap"
	_ "github.com/go-sql-driver/mysql"
)

// User represents a user in our application
type User struct {
	ID        int       `db:"id"`
	Name      string    `db:"name"`
	Email     string    `db:"email"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func ExampleDB_Select() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Select a single user by email
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", dbmap.Args{
		"email": "mulder@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found user: %s (%s)\n", user.Name, user.Email)

	// Select multiple active users
	var activeUsers []User
	err = db.Select(ctx, &activeUsers, "WHERE email LIKE $pattern AND active = $active ORDER BY name LIMIT 2", dbmap.Args{
		"pattern": "%@xfiles.gov",
		"active":  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d active users\n", len(activeUsers))

	// Output:
	// Found user: Fox Mulder (mulder@xfiles.gov)
	// Found 2 active users
}

func ExampleDB_Insert() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Insert a new user - ID and timestamps will be set automatically
	user := &User{
		Name:   "C.G.B Spender",
		Email:  "smokingman@xfiles.gov",
		Active: true,
	}

	err := db.Insert(ctx, user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted user with ID: %d\n", user.ID)
	fmt.Printf("Active: %t\n", user.Active)
	// Output:
	// Inserted user with ID: 5
	// Active: true
}

func ExampleDB_Update() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Update user status
	rowsAffected, err := db.Update(ctx, &User{}, "WHERE email = $email", dbmap.Args{
		"email": "skinner@xfiles.gov",
	}, dbmap.Updates{
		"Active": true,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Activated %d users\n", rowsAffected)
	// Output:
	// Activated 1 users
}

func ExampleDB_UpdateRecord() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Get an existing user
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", dbmap.Args{
		"email": "mulder@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Update the user record - UpdatedAt will be set automatically
	err = db.UpdateRecord(ctx, &user, dbmap.Updates{
		"Name": "Fox Mulder (FBI Agent)",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated user: %s\n", user.Name)
	// Output:
	// Updated user: Fox Mulder (FBI Agent)
}

func ExampleDB_Delete() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Delete users
	rowsAffected, err := db.Delete(ctx, &User{}, "WHERE email LIKE $pattern AND active = $active", dbmap.Args{
		"pattern": "%@xfiles.gov",
		"active":  false,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Deleted %d users\n", rowsAffected)
	// Output:
	// Deleted 1 users
}

func ExampleDB_DeleteRecord() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Get an existing user to delete
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", dbmap.Args{
		"email": "scully@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Delete the specific user record
	rowsAffected, err := db.DeleteRecord(ctx, &user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Deleted %d user\n", rowsAffected)
	// Output:
	// Deleted 1 user
}

func ExampleDB_Transaction() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	err := db.Transaction(ctx, func(tx *dbmap.DB) error {
		// Insert a new user
		user := &User{
			Name:   "Monica Reyes",
			Email:  "reyes@xfiles.gov",
			Active: true,
		}
		if err := tx.Insert(ctx, user); err != nil {
			return err
		}

		// Update another user in the same transaction
		_, err := tx.Update(ctx, &User{}, "WHERE email = $email", dbmap.Args{
			"email": "mulder@xfiles.gov",
		}, dbmap.Updates{
			"Active": false,
		})
		return err
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Transaction completed successfully")
	// Output:
	// Transaction completed successfully
}

func ExampleDB_Query() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Execute a raw query with named parameters
	rows, err := db.Query(ctx, `
		SELECT name, email
		FROM users
		WHERE email LIKE $pattern AND active = $active
		ORDER BY name LIMIT 2
	`, dbmap.Args{
		"pattern": "%@xfiles.gov",
		"active":  true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, email string
		if err := rows.Scan(&name, &email); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s (%s)\n", name, email)
	}
	// Output:
	// Dana Scully (scully@xfiles.gov)
	// Fox Mulder (mulder@xfiles.gov)
}

func ExampleDB_Exec() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := dbmap.New(sqlDB)
	ctx := context.Background()

	// Execute a raw SQL statement
	result, err := db.Exec(ctx, `
		UPDATE users
		SET active = $active
		WHERE email LIKE $pattern
	`, dbmap.Args{
		"active":  false,
		"pattern": "%@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated %d users\n", rowsAffected)
	// Output:
	// Updated 3 users
}
