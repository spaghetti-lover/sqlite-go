package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	lower := strings.ToLower(command)
	switch {
	case lower == ".dbinfo":
		pageSize, numberOfTables, err := dbInfo(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("database page size: ", pageSize)
		fmt.Println("number of tables: ", numberOfTables)

	case lower == ".tables":
		names, err := tableNames(databaseFilePath)
		if err != nil {
			log.Fatal(err)
			return
		}
		fmt.Println(names)
	case strings.HasPrefix(lower, "select count(*) from "):
		parts := strings.Fields(command)
		if len(parts) != 4 {
			log.Fatal("Invalid COUNT query format")
		}
		tableName := parts[len(parts)-1]
		cnt, err := countRows(databaseFilePath, tableName)
		if err != nil {
			log.Fatal(err)
			return
		}

		fmt.Println(cnt)

	case strings.HasPrefix(lower, "select"):
		parts := strings.Fields(command)
		if len(parts) != 4 {
			log.Fatal("Invalid select query format")
		}
		tableName := parts[len(parts)-1]
		colName := parts[1]
		data, err := readDataFromSelect(databaseFilePath, tableName, colName)
		if err != nil {
			log.Fatal(err)
			return
		}
		fmt.Println(data)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
