package main

import (
	"bytes"
	"io"
	"log"
	"os"
)

func countRows(databaseFilePath, tableName string) (int, error) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

	// Read file header to get page size
	header := make([]byte, 100)
	if _, err := databaseFile.Read(header); err != nil {
		log.Fatal(err)
	}
	fH, err := BuildFileHeader(header)
	if err != nil {
		return 0, err
	}

	// Read first page (which contains sqlite_schema)
	if _, err := databaseFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	page := make([]byte, fH.PageSize)
	if _, err := databaseFile.Read(page); err != nil {
		return 0, err
	}

	pH, err := BuildPageHeader(page[100:])
	if err != nil {
		return 0, err
	}

	pageHeaderSize := 8
	if pH.PageType == 2 || pH.PageType == 5 {
		pageHeaderSize = 12
	}
	cellStartIdx := 100 + pageHeaderSize
	cellArray := getCellArray(page, cellStartIdx, int(pH.NumberPageCells))

	var rootpage int
	for _, offset := range cellArray {
		_, rp, ok := parseCellForCount(page, int(offset), tableName)
		if ok {
			rootpage = rp
			break
		}
	}
	if rootpage == 0 {
		log.Fatalf("Table %s not found", tableName)
	}

	// Đọc root page chứa dữ liệu bảng
	dataPage := make([]byte, fH.PageSize)
	offset := int64((rootpage - 1) * int(fH.PageSize))
	if _, err := databaseFile.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	if _, err := databaseFile.Read(dataPage); err != nil {
		return 0, err
	}

	dataPageHeader := parsePageHeader(bytes.NewReader(dataPage))
	return len(dataPageHeader.CellPointers), nil
}
