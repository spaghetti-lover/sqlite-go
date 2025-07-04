package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func readDataFromSelect(databaseFilePath, tableName string, colName string) ([]string, error) {
	// Find the root page
	db, err := os.Open(databaseFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}
	defer db.Close()

	header := make([]byte, 100)
	if _, err := db.Read(header); err != nil {
		return nil, fmt.Errorf("failed to read database header: %w", err)
	}
	fH, err := BuildFileHeader(header)
	if err != nil {
		return nil, fmt.Errorf("failed to build file header: %w", err)
	}
	_, err = db.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start of database file: %w", err)
	}
	page := make([]byte, fH.PageSize)
	if _, err := db.Read(page); err != nil {
		return nil, fmt.Errorf("failed to read first page: %w", err)
	}
	// Parse the CREATE TABLE statement
	pH, err := BuildPageHeader(page[100:])
	if err != nil {
		return nil, fmt.Errorf("failed to build page header: %w", err)
	}
	pageHeaderSize := 8
	if pH.PageType == 2 || pH.PageType == 5 {
		pageHeaderSize = 12
	}
	cellStartIdx := 100 + pageHeaderSize
	cellArray := getCellArray(page, cellStartIdx, int(pH.NumberPageCells))

	var rootpage int
	var createSQL string
	for _, offset := range cellArray {
		rec, _ := parseRecord(page, int(offset))
		if len(rec.Values) < 5 {
			continue
		}
		fmt.Printf("DEBUG: rec.Values = %#v\n", rec.Values)

		if rec.Values[0] == "table" && strings.EqualFold(rec.Values[1], tableName) {
			rootpageStr := strings.TrimSpace(rec.Values[3])
			rp, err := strconv.Atoi(rootpageStr)
			if err != nil {
				return nil, fmt.Errorf("failed to convert root page string to int: %w", err)
			} else {
				rootpage = rp
			}
			createSQL = rec.Values[4]
			break
		}
	}
	if rootpage == 0 || createSQL == "" {
		return nil, fmt.Errorf("table %s not found in database", tableName)
	}
	// Read the table's root page
	colIdx := getColumnIndex(createSQL, colName)
	if colIdx == -1 {
		return nil, fmt.Errorf("column %s not found in table %s", colName, tableName)
	}
	// Read the data from the root page based on the column name
	dataPage := make([]byte, fH.PageSize)
	offset := int64((rootpage - 1) * int(fH.PageSize))
	if _, err := db.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to root page %d: %w", rootpage, err)
	}
	if _, err := db.Read(dataPage); err != nil {
		return nil, fmt.Errorf("failed to read root page %d: %w", rootpage, err)
	}
	dataPageHeader := parsePageHeader(bytes.NewReader(dataPage))
	results := []string{}
	for _, cellPtr := range dataPageHeader.CellPointers {
		rec, err := parseRecord(dataPage, int(cellPtr))
		if err != nil || colIdx >= len(rec.Values) {
			continue
		}
		results = append(results, rec.Values[colIdx])
	}
	return results, nil
}

func getColumnIndex(createStatement string, columnName string) int {
	re := regexp.MustCompile(`(?i)CREATE TABLE \w+\s*\(([^\)]+)\)`)
	matches := re.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return -1
	}
	columns := strings.Split(matches[1], ",")
	for i, c := range columns {
		if strings.Split(strings.TrimSpace(c), " ")[0] == columnName {
			return i
		}
	}
	return -1
}
