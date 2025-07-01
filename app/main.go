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
		printDbInfo(databaseFilePath)

	case lower == ".tables":
		printTableNames(databaseFilePath)

	case strings.HasPrefix(lower, "select count(*) from "):
		parts := strings.Fields(command)
		if len(parts) != 4 {
			log.Fatal("Invalid COUNT query format")
		}
		tableName := parts[len(parts)-1]
		countRows(databaseFilePath, tableName)

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
