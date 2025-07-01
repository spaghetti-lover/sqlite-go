package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func printTableNames(databaseFilePath string) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	page, err := readFirstPage(databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	tableNames, err := extractTableNames(page)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(strings.Join(tableNames, " "))
}