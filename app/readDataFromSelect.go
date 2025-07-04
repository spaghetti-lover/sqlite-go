package main

import (
	"bytes"
	"encoding/binary"
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
	// Scan the table B-tree
	results, err := scanTableBTree(db, fH.PageSize, rootpage, colIdxs, whereColIdx, whereVal, colNames)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func getColumnIndex(createStatement string, columnName string) int {
	re := regexp.MustCompile(`(?i)CREATE TABLE\s+["'\[]?\w+["'\]]?\s*\((.*)\)`)
	matches := re.FindStringSubmatch(createStatement)
	if len(matches) < 2 {
		return -1
	}
	colsDef := matches[1]
	colsDef = strings.ReplaceAll(colsDef, "\n", " ")
	colsDef = strings.ReplaceAll(colsDef, "\t", " ")
	colsDef = strings.ReplaceAll(colsDef, "\r", " ")
	colsDef = regexp.MustCompile(`\s+`).ReplaceAllString(colsDef, " ")
	columns := strings.Split(colsDef, ",")
	for i, c := range columns {
		c = strings.TrimSpace(c)
		colName := strings.Fields(c)
		if len(colName) > 0 {
			cleanCol := strings.Trim(colName[0], `"'[]`)
			if strings.EqualFold(cleanCol, columnName) {
				return i
			}
		}
	}
	return -1
}

func scanTableBTree(db *os.File, pageSize uint16, pageNum int, colIdxs []int, whereColIdx int, whereVal string, colNames []string) ([]string, error) {
	offset := int64((pageNum - 1) * int(pageSize))
	page := make([]byte, pageSize)

	if _, err := db.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	if _, err := db.Read(page); err != nil {
		return nil, err
	}

	pageType := page[0]
	results := []string{}

	switch pageType {
	case 13:
		dataPageHeader := parsePageHeader(bytes.NewReader(page))
		for _, cellPtr := range dataPageHeader.CellPointers {
			rowid, rec, err := parseRecordWithRowid(page, int(cellPtr))
			if err != nil {
				continue
			}
			if whereColIdx != -1 {
				if strings.TrimSpace(strings.ToLower(rec.Values[whereColIdx])) != strings.TrimSpace(strings.ToLower(whereVal)) {
					continue
				}
			}
			values := make([]string, len(colIdxs))
			for i, idx := range colIdxs {
				// Nếu là cột id (INTEGER PRIMARY KEY), lấy rowid
				if strings.EqualFold(colNames[i], "id") {
					values[i] = strconv.Itoa(rowid)
				} else if idx >= len(rec.Values) {
					values[i] = ""
				} else {
					values[i] = rec.Values[idx]
				}
			}
			results = append(results, strings.Join(values, "|"))
		}
	case 5:
		dataPageHeader := parsePageHeader(bytes.NewReader(page))
		for _, cellPtr := range dataPageHeader.CellPointers {
			if int(cellPtr)+4 > len(page) {
				continue // skip invalid cell pointer
			}
			childPageNum := int(binary.BigEndian.Uint32(page[cellPtr : cellPtr+4]))
			childResults, err := scanTableBTree(db, pageSize, childPageNum, colIdxs, whereColIdx, whereVal, colNames)
			if err != nil {
				continue // skip child page if error
			}
			results = append(results, childResults...)
		}
		// right-most pointer nằm ở offset 8-12 của page
		if len(page) >= 12 {
			rightMostPtr := int(binary.BigEndian.Uint32(page[8:12]))
			childResults, err := scanTableBTree(db, pageSize, rightMostPtr, colIdxs, whereColIdx, whereVal, colNames)
			if err == nil {
				results = append(results, childResults...)
			}
		}
	}
	return results, nil
}
