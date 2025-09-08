package dbmap_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

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
	database := getEnv("MYSQL_DATABASE", "dbmap_test")

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
