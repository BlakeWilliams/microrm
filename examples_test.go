package microrm_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/blakewilliams/microrm"
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

// exampleConnectionString returns the database connection string for examples
var exampleConnectionString string

func init() {
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnv("MYSQL_PORT", "3306")
	user := getEnv("MYSQL_USER", "root")
	password := getEnv("MYSQL_PASSWORD", "")
	database := getEnv("MYSQL_DATABASE", "microrm_test")

	exampleConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true", user, password, host, port, database)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func ExampleDB_Select() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)

	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Select a single user by email
	var user User
	err = db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
		"email": "john@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found user: %s (%s)\n", user.Name, user.Email)

	// Select multiple users
	var users []User
	err = db.Select(ctx, &users, "WHERE active = $active ORDER BY name", microrm.Args{
		"active": true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found %d active users\n", len(users))

	// Output:
	// Found user: John Doe (john@example.com)
	// Found 4 active users
}

func ExampleDB_Insert() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Insert a new user - ID and timestamps will be set automatically
	user := &User{
		Name:   "Jane Smith",
		Email:  "jane@example.com",
		Active: true,
	}

	err = db.Insert(ctx, user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted user with ID: %d\n", user.ID)
	fmt.Printf("Active: %t\n", user.Active)
	// Output:
	// Inserted user with ID: 6
	// Active: true
}

func ExampleDB_Update() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Update multiple users
	rowsAffected, err := db.Update(ctx, &User{}, "WHERE active = $active", microrm.Args{
		"active": false,
	}, microrm.Updates{
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
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Get a user
	var user User
	err = db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
		"email": "john@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Update the user record - UpdatedAt will be set automatically
	err = db.UpdateRecord(ctx, &user, microrm.Updates{
		"Name": "John Doe Jr.",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated user: %s\n", user.Name)
	// Output:
	// Updated user: John Doe Jr.
}

func ExampleDB_Delete() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Delete inactive users
	rowsAffected, err := db.Delete(ctx, &User{}, "WHERE active = $active", microrm.Args{
		"active": false,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Deleted %d inactive users\n", rowsAffected)
	// Output:
	// Deleted 0 inactive users
}

func ExampleDB_DeleteRecord() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Get a user to delete
	var user User
	err = db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
		"email": "delete-me@example.com",
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
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	err = db.Transaction(ctx, func(tx *microrm.DB) error {
		// Insert a new user
		user := &User{
			Name:   "Transaction User",
			Email:  "tx@example.com",
			Active: true,
		}
		if err := tx.Insert(ctx, user); err != nil {
			return err
		}

		// Update another user in the same transaction
		_, err := tx.Update(ctx, &User{}, "WHERE email = $email", microrm.Args{
			"email": "other@example.com",
		}, microrm.Updates{
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
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Execute a raw query with named parameters
	rows, err := db.Query(ctx, `
		SELECT name, email
		FROM users
		WHERE active = $active
		ORDER BY name LIMIT 2
	`, microrm.Args{
		"active": true,
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
	// Alice Smith (alice@example.com)
	// Bob Johnson (bob@example.com)
}

func ExampleDB_Exec() {
	// Setup database connection
	sqlDB, err := sql.Open("mysql", exampleConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDB.Close()

	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Execute a raw SQL statement
	result, err := db.Exec(ctx, `
		UPDATE users
		SET updated_at = NOW()
		WHERE active = $active
	`, microrm.Args{
		"active": true,
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
	// Updated 5 users
}
