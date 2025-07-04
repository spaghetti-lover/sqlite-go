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

func readDataFromSelect(databaseFilePath, tableName string, colNames []string, whereCol string, whereVal string) ([]string, error) {
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
		// fmt.Printf("DEBUG: rec.Values = %#v\n", rec.Values)

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
	colIdxs := make([]int, len(colNames))
	for i, col := range colNames {
		colIdxs[i] = getColumnIndex(createSQL, col)
		if colIdxs[i] == -1 {
			return nil, fmt.Errorf("column %s not found in table %s", col, tableName)
		}
	}
	whereColIdx := -1
	if whereCol != "" {
		whereColIdx = getColumnIndex(createSQL, whereCol)
		if whereColIdx == -1 {
			return nil, fmt.Errorf("where column %s not found in table %s", whereCol, tableName)
		}
	}
	// Read the data from the root page based on the column name
	dataPage := make([]byte, fH.PageSize)
	offset := int64((rootpage - 1) * int(fH.PageSize))
	if _, err := db.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to root page: %w", err)
	}
	if _, err := db.Read(dataPage); err != nil {
		return nil, fmt.Errorf("failed to read root page: %w", err)
	}
	dataPageHeader := parsePageHeader(bytes.NewReader(dataPage))
	results := []string{}
	for _, cellPtr := range dataPageHeader.CellPointers {
		rec, err := parseRecord(dataPage, int(cellPtr))
		if err != nil {
			continue
		}

		if whereColIdx != -1 {
			if whereColIdx >= len(rec.Values) || rec.Values[whereColIdx] != whereVal {
				continue
			}
		}

		values := make([]string, len(colIdxs))
		for i, idx := range colIdxs {
			if idx >= len(rec.Values) {
				values[i] = ""
			} else {
				values[i] = rec.Values[idx]
			}
		}
		results = append(results, strings.Join(values, "|"))
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
