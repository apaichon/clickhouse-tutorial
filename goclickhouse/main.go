package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

// Define a struct to represent a person
type Person struct {
	ID          string `json:"id"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	DateOfBirth string `json:"date_of_birth"`
	LaserID     string `json:"laser_id"`
}

func main() {

		// Load environment variables from .env file
		err := godotenv.Load()
		if err != nil {
			log.Fatalf("Error loading .env file: %v", err)
		}

	// Get ClickHouse connection parameters from environment variables
	clickhouseHost := os.Getenv("CLICKHOUSE_HOST")
	clickhousePort := os.Getenv("CLICKHOUSE_PORT")
	clickhouseUsername := os.Getenv("CLICKHOUSE_USERNAME")
	clickhousePassword := os.Getenv("CLICKHOUSE_PASSWORD")

	// Initialize Gin
	r := gin.Default()

	// Construct the ClickHouse connection string
	connectionString := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s", clickhouseHost, clickhousePort, clickhouseUsername, clickhousePassword)


	// Connect to ClickHouse
	db, err := sql.Open("clickhouse", connectionString)
	if err != nil {
		fmt.Println("Failed to connect to ClickHouse:", err)
		return
	}
	defer db.Close()

	// Define routes
	r.POST("/people", func(c *gin.Context) {
		// Bind JSON request body to Person struct
		var person Person
		if err := c.BindJSON(&person); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Execute INSERT query
		_, err := db.Exec("INSERT INTO people (id, first_name, last_name, dateOfBirth, laser_id) VALUES (?, ?, ?, ?, ?)",
			person.ID, person.FirstName, person.LastName, person.DateOfBirth, person.LaserID)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusCreated, gin.H{"message": "Person created successfully"})
	})

	r.GET("/people/:id", func(c *gin.Context) {
		id := c.Param("id")

		// Execute SELECT query
		rows, err := db.Query("SELECT * FROM people WHERE id = ?", id)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()

		// Parse result rows into Person struct
		var person Person
		for rows.Next() {
			if err := rows.Scan(&person.ID, &person.FirstName, &person.LastName, &person.DateOfBirth, &person.LaserID); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}

		c.JSON(http.StatusOK, person)
	})

	// Run Gin server
	r.Run(":9001")
}
