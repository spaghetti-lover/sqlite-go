package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		printDbInfo(databaseFilePath)
	case ".tables":
		printTableNames(databaseFilePath)
	case "SELECT COUNT(*) FROM apples":
		parts := strings.Split("SELECT COUNT(*) FROM apples", " ")
		tableName := parts[len(parts)-1]
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
			log.Fatal(err)
		}

		// Read first page
		_, err = databaseFile.Seek(0, 0)
		if err != nil {
			log.Fatal(err)
		}
		page := make([]byte, fH.PageSize)
		if _, err := databaseFile.Read(page); err != nil {
			log.Fatal(err)
		}

		pH, err := BuildPageHeader(page[100:])
		if err != nil {
			log.Fatal(err)
			return
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
		// Đọc page dữ liệu gốc của bảng
		dataPage := make([]byte, fH.PageSize)
		offset := int64((rootpage - 1) * int(fH.PageSize)) // rootpage tính từ 1
		if _, err := databaseFile.Seek(offset, io.SeekStart); err != nil {
			log.Fatal(err)
		}
		if _, err := databaseFile.Read(dataPage); err != nil {
			log.Fatal(err)
		}

		dataPageHeader := parsePageHeader(bytes.NewReader(dataPage))
		fmt.Println(len(dataPageHeader.CellPointers))

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func parseCellForCount(page []byte, offset int, tableName string) (string, int, bool) {
	_, newOffset := parseVarInt(page, offset)   // payload size
	_, newOffset = parseVarInt(page, newOffset) // rowid

	startOfRecord := newOffset
	recordHeaderSize, newOffset := parseVarInt(page, newOffset)

	headerToRead := int(recordHeaderSize) - (newOffset - int(startOfRecord))
	serialTypeCodes := make([]uint64, 0)
	for headerToRead > 0 {
		var serialCode uint64
		oldOffset := newOffset
		serialCode, newOffset = parseVarInt(page, newOffset)
		serialTypeCodes = append(serialTypeCodes, serialCode)
		headerToRead -= newOffset - oldOffset
	}

	values := []string{}
	intValues := []uint64{}
	for _, code := range serialTypeCodes {
		switch {
		case code >= 13 && code%2 == 1:
			textLen := int((code - 13) / 2)
			if newOffset+textLen > len(page) {
				return "", 0, false
			}
			text := string(page[newOffset : newOffset+textLen])
			values = append(values, text)
			newOffset += textLen
		case code == 1 || code == 2 || code == 3 || code == 4 || code == 5 || code == 6 || code == 8 || code == 9:
			// These are integer types of various sizes
			byteSize := map[uint64]int{1: 1, 2: 2, 3: 3, 4: 4, 5: 6, 6: 8, 8: 0, 9: 8}[code]
			valBytes := make([]byte, 8)
			copy(valBytes[8-byteSize:], page[newOffset:newOffset+byteSize])
			val := binary.BigEndian.Uint64(valBytes)
			intValues = append(intValues, val)
			newOffset += byteSize
		default:
			// skip unsupported types for now
			return "", 0, false
		}
	}

	if len(values) > 1 && len(intValues) > 0 && values[0] == "table" && values[1] == tableName {
		return values[1], int(intValues[0]), true
	}

	return "", 0, false
}

func parsePageHeader(r io.Reader) PageHeader {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		log.Fatalf("Failed to read page header: %v", err)
	}

	pageType := header[0]
	firstFreeblock := binary.BigEndian.Uint16(header[1:3])
	numCells := binary.BigEndian.Uint16(header[3:5])
	cellContentArea := binary.BigEndian.Uint16(header[5:7])
	fragmentedFreeBytes := header[7]

	cellPointers := make([]uint16, numCells)
	for i := 0; i < int(numCells); i++ {
		ptr := make([]byte, 2)
		if _, err := r.Read(ptr); err != nil {
			log.Fatalf("Failed to read cell pointer: %v", err)
		}
		cellPointers[i] = binary.BigEndian.Uint16(ptr)
	}

	return PageHeader{
		PageType:            pageType,
		FirstFreeblock:      firstFreeblock,
		NumberOfCells:       numCells,
		CellContentArea:     cellContentArea,
		FragmentedFreeBytes: fragmentedFreeBytes,
		CellPointers:        cellPointers,
	}
}
