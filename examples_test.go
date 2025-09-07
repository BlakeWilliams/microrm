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

func ExampleDB_Select() {
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Select a single user by email
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
		"email": "mulder@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found user: %s (%s)\n", user.Name, user.Email)

	// Select multiple active users
	var activeUsers []User
	err = db.Select(ctx, &activeUsers, "WHERE email LIKE $pattern AND active = $active ORDER BY name LIMIT 2", microrm.Args{
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
	db := microrm.New(sqlDB)
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
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Update user status
	rowsAffected, err := db.Update(ctx, &User{}, "WHERE email = $email", microrm.Args{
		"email": "skinner@xfiles.gov",
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
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Get an existing user
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
		"email": "mulder@xfiles.gov",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Update the user record - UpdatedAt will be set automatically
	err = db.UpdateRecord(ctx, &user, microrm.Updates{
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
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Delete users
	rowsAffected, err := db.Delete(ctx, &User{}, "WHERE email LIKE $pattern AND active = $active", microrm.Args{
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
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Get an existing user to delete
	var user User
	err := db.Select(ctx, &user, "WHERE email = $email", microrm.Args{
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
	db := microrm.New(sqlDB)
	ctx := context.Background()

	err := db.Transaction(ctx, func(tx *microrm.DB) error {
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
		_, err := tx.Update(ctx, &User{}, "WHERE email = $email", microrm.Args{
			"email": "mulder@xfiles.gov",
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
	sqlDB, cleanup := setupDB()
	defer cleanup()
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Execute a raw query with named parameters
	rows, err := db.Query(ctx, `
		SELECT name, email
		FROM users
		WHERE email LIKE $pattern AND active = $active
		ORDER BY name LIMIT 2
	`, microrm.Args{
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
	db := microrm.New(sqlDB)
	ctx := context.Background()

	// Execute a raw SQL statement
	result, err := db.Exec(ctx, `
		UPDATE users
		SET updated_at = NOW()
		WHERE email LIKE $pattern
	`, microrm.Args{
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
	// Updated 4 users
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func setupDB() (*sql.DB, func()) {
	host := getEnv("MYSQL_HOST", "localhost")
	port := getEnv("MYSQL_PORT", "3306")
	user := getEnv("MYSQL_USER", "root")
	password := getEnv("MYSQL_PASSWORD", "")
	database := getEnv("MYSQL_DATABASE", "microrm_test")

	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, password, host, port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true", user, password, host, port, database)

	if err := setupExampleDatabase(rootDSN, dsn, database); err != nil {
		log.Fatalf("Failed to setup example database: %v", err)
	}

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	cleanup := func() {
		// Truncate tables on cleanup
		sqlDB.Exec("TRUNCATE TABLE users")
		sqlDB.Close()
	}

	return sqlDB, cleanup
}

func setupExampleDatabase(rootDSN, dsn, database string) error {
	rootDB, err := sql.Open("mysql", rootDSN)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer rootDB.Close()

	_, err = rootDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to example database: %w", err)
	}
	defer sqlDB.Close()

	if err = setupExampleTables(sqlDB); err != nil {
		return fmt.Errorf("failed to setup example tables: %w", err)
	}

	if err = insertExampleData(sqlDB); err != nil {
		return fmt.Errorf("failed to insert example data: %w", err)
	}

	return nil
}

func setupExampleTables(db *sql.DB) error {
	dropSQL := `DROP TABLE IF EXISTS key_values, users;`
	if _, err := db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop existing tables: %w", err)
	}

	createUsersSQL := `
		CREATE TABLE users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP NULL,
			updated_at TIMESTAMP NULL
		)
	`
	if _, err := db.Exec(createUsersSQL); err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	return nil
}

func insertExampleData(db *sql.DB) error {
	// Truncate tables first to ensure clean state
	_, err := db.Exec("TRUNCATE TABLE users")
	if err != nil {
		return fmt.Errorf("failed to truncate users table: %w", err)
	}

	userData := []struct {
		name, email string
		active      bool
	}{
		{"Fox Mulder", "mulder@xfiles.gov", true},
		{"Dana Scully", "scully@xfiles.gov", true},
		{"Walter Skinner", "skinner@xfiles.gov", false},
		{"John Doggett", "doggett@xfiles.gov", true},
	}

	for _, user := range userData {
		_, err := db.Exec("INSERT INTO users (name, email, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
			user.name, user.email, user.active, time.Now().UTC(), time.Now().UTC())
		if err != nil {
			return fmt.Errorf("failed to insert user data: %w", err)
		}
	}

	return nil
}
