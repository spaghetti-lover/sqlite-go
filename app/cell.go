package main

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
