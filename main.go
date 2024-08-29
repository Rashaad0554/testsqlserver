package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
)

// SQL Server connection details
var server = "localhost"       // Your SQL Server hostname
var mssql_port = 1433          // Default SQL Server port
var user = "UofGIoT"           // Your SQL Server username
var passwd = "Iq1sAd7AVVK5UUR" // Your SQL Server password
var database = "emqx_data"     // Your database name

var db *sql.DB

func main() {
	// Form the connection string
	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;", server, user, passwd, mssql_port, database)

	// Open a connection to SQL Server
	var err error
	db, err = sql.Open("sqlserver", connectionString)
	if err != nil {
		log.Fatal("Error opening database:", err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to database:", err)
	}

	fmt.Println("Connected to SQL Server successfully!")
	selectVersion()
}

func selectVersion() {
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		log.Fatal("Error pinging database: ", err)
	}

	var result string
	err = db.QueryRowContext(ctx, "SELECT @@version").Scan(&result)
	if err != nil {
		log.Fatal("Scan failed: ", err)
	}
	fmt.Printf("%s\n", result)
}