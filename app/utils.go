package main

import (
	"encoding/binary"
	"io"
	"log"
	"os"
)

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
	PageNumber          uint32 // Page number in the database file
	PageType            uint8  // Page type (0: free, 1: leaf, 2: interior, 3: overflow, 4: table, 5: index)
	NumberPageCells     uint16 // Number of cells in the page
	FirstFreeblock      uint16
	NumberOfCells       uint16
	CellContentArea     uint16
	FragmentedFreeBytes byte
	CellPointers        []uint16
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

func readFirstPage(databaseFile *os.File) ([]byte, error) {
	header := make([]byte, 100)
	if _, err := databaseFile.Read(header); err != nil {
		return nil, err
	}

	fH, err := BuildFileHeader(header)
	if err != nil {
		return nil, err
	}

	_, err = databaseFile.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	page := make([]byte, fH.PageSize)
	if _, err := databaseFile.Read(page); err != nil {
		return nil, err
	}
	return page, nil
}

func extractTableNames(page []byte) ([]string, error) {
	pH, err := BuildPageHeader(page[100:])
	if err != nil {
		return nil, err
	}

	pageHeaderSize := 8
	if pH.PageType == 2 || pH.PageType == 5 {
		pageHeaderSize = 12
	}
	cellStartIdx := 100 + pageHeaderSize
	cellArray := getCellArray(page, cellStartIdx, int(pH.NumberPageCells))

	tableNames := []string{}
	for _, cell := range cellArray {
		tableName := parseCell(page, int(cell))
		if tableName != "" {
			tableNames = append(tableNames, tableName)
		}
	}
	return tableNames, nil
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
