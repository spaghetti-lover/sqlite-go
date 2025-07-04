package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
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

type Record struct {
	Values []string
}

func parseRecord(data []byte, offset int) (Record, error) {
	pos := offset

	// 1. Parse payload size (varint)
	_, n := readVarint(data[pos:])
	pos += n

	// 2. Parse rowid (varint) if present (for table b-tree leaf cells)
	// For sqlite_schema, rowid is present. For index b-tree, it's not.
	// We'll try to parse it, but if the next varint is too large, it will just be part of the header.
	// This is safe for most practical cases.
	_, n = readVarint(data[pos:])
	pos += n

	// 3. Parse record header size (varint)
	headerStart := pos
	headerSize, n := readVarint(data[pos:])
	pos += n

	// 4. Parse serial types
	serialTypes := []int{}
	headerBytesRead := pos - headerStart
	for headerBytesRead < int(headerSize) {
		serial, n := readVarint(data[pos:])
		serialTypes = append(serialTypes, int(serial))
		pos += n
		headerBytesRead += n
	}
	// 5 . Parse values based on serial types
	values := []string{}
	bodyPos := pos
	for _, st := range serialTypes {
		val, size := readValueBySerialType(data[bodyPos:], st)
		values = append(values, val)
		bodyPos += size
	}
	return Record{Values: values}, nil
}

// Trả về giá trị varint và số byte đã đọc
func readVarint(data []byte) (int, int) {
	var result int
	for i := 0; i < 9 && i < len(data); i++ {
		b := data[i]
		result = (result << 7) | int(b&0x7F)
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	// Nếu không gặp byte kết thúc, byte thứ 9 là toàn bộ
	if len(data) >= 9 {
		b := data[8]
		result = (result << 8) | int(b)
		return result, 9
	}
	return 0, 0 // lỗi
}

func readValueBySerialType(data []byte, serialType int) (string, int) {
	switch serialType {
	case 0:
		return "NULL", 0
	case 1:
		if len(data) < 1 {
			return "", 0
		}
		return strconv.Itoa(int(int8(data[0]))), 1
	case 2:
		if len(data) < 2 {
			return "", 0
		}
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(data)))), 2
	case 3:
		if len(data) < 3 {
			return "", 0
		}
		val := int(data[0])<<16 | int(data[1])<<8 | int(data[2])
		return strconv.Itoa(val), 3
	case 4:
		if len(data) < 4 {
			return "", 0
		}
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(data)))), 4
	case 5:
		if len(data) < 6 {
			return "", 0
		}
		val := int64(data[0])<<40 | int64(data[1])<<32 | int64(data[2])<<24 | int64(data[3])<<16 | int64(data[4])<<8 | int64(data[5])
		return strconv.FormatInt(val, 10), 6
	case 6:
		if len(data) < 8 {
			return "", 0
		}
		val := binary.BigEndian.Uint64(data)
		return strconv.FormatInt(int64(val), 10), 8
	case 7:
		if len(data) < 8 {
			return "", 0
		}
		bits := binary.BigEndian.Uint64(data)
		f := math.Float64frombits(bits)
		return strconv.FormatFloat(f, 'f', -1, 64), 8
	case 8:
		return "0", 0
	case 9:
		return "1", 0
	default:
		if serialType >= 12 {
			length := 0
			if serialType%2 == 0 {
				length = (serialType - 12) / 2 // BLOB
				if len(data) < length {
					return "", 0
				}
				return fmt.Sprintf("BLOB[%d]", length), length
			} else {
				length = (serialType - 13) / 2 // TEXT
				if len(data) < length {
					return "", 0
				}
				return string(data[:length]), length
			}
		}
	}
	return "", 0 // fallback
}

func parseRecordWithRowid(data []byte, offset int) (int, Record, error) {
	// 1. Parse payload size (varint)
	_, n := readVarint(data[offset:])
	pos := offset + n

	// 2. Parse rowid (varint)
	rowid, n2 := readVarint(data[pos:])
	pos += n2

	// 3. Parse record header size (varint)
	headerStart := pos
	headerSize, n3 := readVarint(data[pos:])
	pos += n3

	// 4. Parse serial types
	serialTypes := []int{}
	headerBytesRead := pos - headerStart
	for headerBytesRead < int(headerSize) {
		serial, n := readVarint(data[pos:])
		serialTypes = append(serialTypes, int(serial))
		pos += n
		headerBytesRead += n
	}
	// 5. Parse values based on serial types
	values := []string{}
	bodyPos := pos
	for _, st := range serialTypes {
		val, size := readValueBySerialType(data[bodyPos:], st)
		values = append(values, val)
		bodyPos += size
	}
	return rowid, Record{Values: values}, nil
}
