package main

import (
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
var database = "emqx_data_test"     // Your database name

var db *sql.DB

type Message struct {
	topic_sensor_name string
	measurement       string
}

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

	fmt.Printf("\n\n\n")

	msg1 := Message{"topic1", "1000"}
	if tableInsert(db, msg1) == 0 {
		fmt.Printf("Uh oh 1!")
	} else {
		fmt.Printf("msg1 added successfully!\n")
	}
	msg2 := Message{"topic2", "2000"}
	if tableInsert(db, msg2) == 0 {
		fmt.Printf("Uh oh 2!")
	} else {
		fmt.Printf("msg2 added successfully!\n")
	}
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

func tableInsert(db *sql.DB, message Message) int {
	topicIDQuery := `SELECT topicID FROM Topics WHERE topicName = @topicName`
	row := db.QueryRow(topicIDQuery, sql.Named("topicName", message.topic_sensor_name))

	var logID int
	logInsertQuery := `INSERT INTO Logs (topicID, measurement) VALUES (@topicID, @measurement); SELECT SCOPE_IDENTITY();`

	var topicID int
	err := row.Scan(&topicID)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no rows are returned
			topicInsertQuery := `INSERT INTO Topics (topicName) VALUES (@topicName); SELECT SCOPE_IDENTITY();`
			err := db.QueryRow(topicInsertQuery, sql.Named("topicName", message.topic_sensor_name)).Scan(&topicID)
			if err != nil {
				log.Fatal("1: ", err)
			}

			err = db.QueryRow(logInsertQuery, sql.Named("topicID", topicID), sql.Named("measurement", message.measurement)).Scan(&logID)
			if err != nil {
				log.Fatal("2: ", err)
			}

			return int(logID)
		} else {
			log.Fatal("3: ", err) // Handle other possible errors
			return 0
		}
	} else {
		// Continue processing with the retrieved topicID
		err := db.QueryRow(logInsertQuery, sql.Named("topicID", topicID), sql.Named("measurement", message.measurement)).Scan(&logID)
		if err != nil {
			log.Fatal("4: ", err)
		}

		return int(logID)
	}
}
