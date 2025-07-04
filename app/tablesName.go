package main

import (
	"os"
	"strings"
)

func tableNames(databaseFilePath string) (string, error) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		return "", err
	}
	defer databaseFile.Close()

	page, err := readFirstPage(databaseFile)
	if err != nil {
		return "", err
	}

	tableNames, err := extractTableNames(page)
	if err != nil {
		return "", err
	}
	return strings.Join(tableNames, " "), nil
}