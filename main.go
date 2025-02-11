package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type Vehicle struct {
	Vehicle string
}

func getMySQL() *sql.DB {
	db, err := sql.Open("mysql", "root:root@(127.0.0.1:3306)/GTA_data_db?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func main() {
	db := getMySQL()

	file, err := os.Open("C:\\Users\\visha\\Downloads\\archive\\vehicle_links.csv")
	if err != nil {
		log.Fatal(err)
	}
	df := csv.NewReader(file)
	data,_ := df.ReadAll()
	var vehicles []Vehicle
	for _, value := range data {
		vehicles = append(vehicles, Vehicle{Vehicle: value[0]})
	}
 
	for i := 1; i < len(vehicles); i++ { 
		db.Exec("Create Table vehicle_links (vehicle) values (?)", vehicles[i].Vehicle)
	}

	fmt.Println("Data Inserted")
}
