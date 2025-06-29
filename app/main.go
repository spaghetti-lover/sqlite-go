package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}
		header := make([]byte, 100)
		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}
		fH, err := BuildFileHeader(header)
		if err != nil {
			fmt.Println(err)
			panic(1)
		}
		databaseFile.Close()

		databaseFile, err = os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		page := make([]byte, fH.PageSize)
		_, err = databaseFile.Read(page)
		if err != nil {
			log.Fatal(err)
		}

		pH, _ := BuildPageHeader(page[100:])

		// leaf page
		pageHeaderSize := 8
		// 8 bytes
		if pH.PageType == 2 || pH.PageType == 5 {
			// interior page
			pageHeaderSize = 12
		}

		cellStartIdx := 100 + pageHeaderSize
		cellArray := getCellArray(page, cellStartIdx, int(pH.NumberPageCells*2))

		tableNames := []string{}
		for _, cell := range cellArray {
			tableName := parseCell(page, int(cell))
			if tableName != "sqlite_sequence" {
				tableNames = append(tableNames, tableName)
			}
		}
		fmt.Println(strings.Join(tableNames, " "))

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

func getCellArray(page []byte, offset int, size int) []uint16 {
	result := make([]uint16, 0)

	for i := 2; i <= size; i += 2 {
		var val uint16
		err := binary.Read(bytes.NewReader(page[i+offset-2:i+offset]), binary.BigEndian, &val)
		if err != nil {
			panic(1)
		}
		result = append(result, val)
	}
	return result
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

type FileHeader struct {
	PageSize uint16
	// Bạn có thể thêm trường khác nếu muốn mở rộng
}

func BuildFileHeader(header []byte) (*FileHeader, error) {
	if len(header) < 18 {
		return nil, fmt.Errorf("header too short")
	}
	pageSize := binary.BigEndian.Uint16(header[16:18])
	return &FileHeader{
		PageSize: pageSize,
	}, nil
}

type PageHeader struct {
	PageType            byte
	FirstFreeBlock      uint16
	NumberPageCells     uint16
	StartOfCellContent  uint16
	FragmentedFreeBytes byte
}

func BuildPageHeader(header []byte) (*PageHeader, error) {
	if len(header) < 8 {
		return nil, fmt.Errorf("page header too short")
	}
	return &PageHeader{
		PageType:            header[0],
		FirstFreeBlock:      binary.BigEndian.Uint16(header[1:3]),
		NumberPageCells:     binary.BigEndian.Uint16(header[3:5]),
		StartOfCellContent:  binary.BigEndian.Uint16(header[5:7]),
		FragmentedFreeBytes: header[7],
	}, nil
}
