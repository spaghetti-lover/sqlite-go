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
		if len(parts) < 4 {
			log.Fatal("Invalid select query format")
		}
		// Tìm vị trí "from"
		fromIdx := -1
		for i, p := range parts {
			if strings.ToLower(p) == "from" {
				fromIdx = i
				break
			}
		}
		if fromIdx == -1 || fromIdx < 2 {
			log.Fatal("Invalid select query format")
		}
		tableName := parts[fromIdx+1]
		// Ghép lại các cột từ sau SELECT đến trước FROM
		colStr := strings.Join(parts[1:fromIdx], " ")
		cols := strings.Split(colStr, ",")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}

		// Parse WHERE nếu có
		whereCol := ""
		whereVal := ""
		for i, p := range parts {
			if strings.ToLower(p) == "where" && i+3 <= len(parts) {
				whereCol = parts[i+1]
				whereVal = strings.Join(parts[i+3:], " ")
				whereVal = strings.Trim(whereVal, "'")
				break
			}
		}
		var data []string
		var err error
		if whereCol != "" {
			data, err = readDataFromSelect(databaseFilePath, tableName, cols, whereCol, whereVal)
		} else {
			data, err = readDataFromSelect(databaseFilePath, tableName, cols, "", "")
		}
		if err != nil {
			log.Fatal(err)
			return
		}
		for _, value := range data {
			fmt.Println(value)
		}
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
