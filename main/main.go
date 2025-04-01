package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Data struct {
	Number     int
	Domain     string
	PageNumber float64
}

func getMySQL() *sql.DB {
	db, err := sql.Open("mysql", "root:root@(127.0.0.1:3306)/10mil_db?parseTime=true")
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
		return nil
	}

	if err = db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
		return nil
	}

	return db
}

func main() {
	db := getMySQL()
	if db == nil {
		return
	}
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS tenmil (
		Number   INT,
		Domain    VARCHAR(255),
		pageNumber DECIMAL(10,2)
	)`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	file, err := os.Open("D:\\go\\src\\.go\\csv-databas-1\\top10milliondomains.csv")
	if err != nil {
		log.Fatal(err)
	}  
	df := csv.NewReader(file)
	data, err := df.ReadAll()
	if err != nil {
		log.Fatal("Failed to read CSV file:", err)
	}
	 
	
	for _, files := range data {
		fmt.Printf("Processing file: %s\n", files)
	}

	for i, row := range data {
		if i == 0 { 
			continue 
		}
	
		if len(row) < 3 {
			log.Println("Skipping malformed row:", row)
			continue
		}
	
		Number, err := strconv.Atoi(strings.TrimSpace(row[0]))
		if err != nil {
			log.Println("Failed to parse Number:", row[0], err)
			continue
		}
	
		PageNumber, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
		if err != nil {
			log.Println("Failed to parse PageNumber:", row[2], err)
			continue
		}
	
		_, err = db.Exec(`INSERT INTO tenmil (Number, Domain, PageNumber) VALUES (?, ?, ?)`, Number, row[1], PageNumber)
		if err != nil {
			log.Println("Failed to insert record:", err)
		}
	}

	fmt.Println("Finished processing")

	startTime := time.Now()
	fmt.Printf("Start Time: %s\n", startTime.Format(time.RFC3339))
	fmt.Println("All Data Inserted Successfully!")
	
}
