package main

import (
	"fmt"
	"os"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		printDbInfo(databaseFilePath)
	case ".tables":
		printTableNames(databaseFilePath)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
