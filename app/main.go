package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 105)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		var numberOfTable uint16
		if err := binary.Read(bytes.NewReader(header[103:105]), binary.BigEndian, &numberOfTable); err != nil {
			fmt.Println("Faield to read numberOfTable: ", err)
			return
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		// Info of table named 'sqlite_schema'
		fmt.Printf("database page size: %v", pageSize)
		fmt.Printf("number of tables: %v", numberOfTable)

	case ".tables":
		// Read file database
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer databaseFile.Close()

		//Read the first 100 bytes to get the file header
		header := make([]byte, 100)
		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		// Read the page size from the file header
		fH, err := BuildFileHeader(header)
		if err != nil {
			log.Fatal(err)
		}

		// Comeback to the beginning of the file
		_, err = databaseFile.Seek(0, 0)
		if err != nil {
			log.Fatal(err)
		}

		// Read the next 100 bytes to get the cell array
		page := make([]byte, fH.PageSize)
		_, err = databaseFile.Read(page)
		if err != nil {
			log.Fatal(err)
		}

		// Read the cell array from the page
		pH, err := BuildPageHeader(page[100:])
		if err != nil {
			log.Fatal(err)
		}

		// Read the offset of the cell array
		pageHeaderSize := 8
		if pH.PageType == 2 || pH.PageType == 5 {
			pageHeaderSize = 12
		}
		cellStartIdx := 100 + pageHeaderSize
		cellArray := getCellArray(page, cellStartIdx, int(pH.NumberPageCells))

		// Print the table names
		tableNames := []string{}
		for _, cell := range cellArray {
			tableName := parseCell(page, int(cell))
			if tableName != "" {
				tableNames = append(tableNames, tableName)
			}
		}
		fmt.Println(strings.Join(tableNames, " "))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

type FileHeader struct {
	PageSize uint16 // Page size in bytes
}

func BuildFileHeader(header []byte) (FileHeader, error) {
	var fH FileHeader

	if len(header) < 18 {
		return fH, io.ErrShortBuffer
	}

	fH.PageSize = binary.BigEndian.Uint16(header[16:18])

	return fH, nil
}

type PageHeader struct {
	PageNumber      uint32 // Page number in the database file
	PageType        uint8  // Page type (0: free, 1: leaf, 2: interior, 3: overflow, 4: table, 5: index)
	NumberPageCells uint16 // Number of cells in the page
}

func BuildPageHeader(pageHeader []byte) (PageHeader, error) {
	var pH PageHeader

	if len(pageHeader) < 12 {
		return pH, io.ErrShortBuffer
	}
	pH.PageType = pageHeader[0]
	pH.PageNumber = binary.BigEndian.Uint32(pageHeader[8:12])
	pH.NumberPageCells = binary.BigEndian.Uint16(pageHeader[3:5])

	return pH, nil
}

func getCellArray(page []byte, cellStartIdx int, numberOfCells int) []uint16 {
	cellArray := make([]uint16, numberOfCells)

	for i := 0; i < numberOfCells; i++ {
		cellOffset := cellStartIdx + i*2
		if cellOffset+2 > len(page) {
			break
		}
		cellArray[i] = binary.BigEndian.Uint16(page[cellOffset : cellOffset+2])
	}

	return cellArray
}

func parseCell(page []byte, offset int) string {
	// fmt.Printf("\nstart of cell: %0x\n", offset)
	// recordSize, newOffset := parseVarInt(page, offset)
	// rowId, newOffset := parseVarInt(page, newOffset)
	_, newOffset := parseVarInt(page, offset)
	_, newOffset = parseVarInt(page, newOffset)

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

	// fmt.Printf("record size: %d\n", recordSize)
	// fmt.Printf("row id: %d\n", rowId)
	// fmt.Printf("record header size: %d\n", recordHeaderSize)
	// fmt.Printf("record serial code: %v\n", serialTypeCodes)

	//startOfRecord += int(recordHeaderSize)
	// endOfRecord := startOfRecord + int(recordSize)
	// recordBody := page[newOffset:endOfRecord]
	// fmt.Printf("record binary: %0b\n", recordBody)
	// fmt.Printf("record hex:    %0x\n", recordBody)
	// fmt.Printf("record string: %s\n", recordBody)
	// fmt.Printf("record string length in bytes: %d\n", len(recordBody))

	table_name_start := newOffset + int((serialTypeCodes[0]-13)/2)
	table_name_end := table_name_start + int((serialTypeCodes[1]-13)/2)
	tableName := string(page[table_name_start:table_name_end])

	return tableName
}

// TODO: only use last 7 bits, properly put it in a uint64 after you have the byte slice?
// if 9 use all 8 bits
// return uint64 (varint), int (new offset)
func parseVarInt(page []byte, offset int) (uint64, int) {
	var result uint64
	tempBytes := make([]byte, 0)
	idx := offset
	count := 0
	for {
		val := page[idx]
		tempBytes = append(tempBytes, val)
		// if we hit the end bit IE msb is a 0
		mask := byte(1) << 7
		if val&mask == 0 {
			break
		}
		idx += 1
		count += 1
		if count == 9 {
			break
		}
	}

	shiftAmount := 0
	for i := len(tempBytes) - 1; i >= 0; i-- {
		// get rid of MSB unless at end
		if i != 8 {
			tempBytes[i] = tempBytes[i] << 1
			tempBytes[i] = tempBytes[i] >> 1
		}
		temp := uint64(tempBytes[i]) << shiftAmount
		result = result | temp

		// increment shift amount
		if i == 8 {
			shiftAmount += 8
		} else {
			shiftAmount += 7
		}
	}

	return result, offset + len(tempBytes)
}
