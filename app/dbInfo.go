package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

func dbInfo(databaseFilePath string) (uint16, uint16, error) {
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		return 0, 0, err
	}

	header := make([]byte, 105)

	_, err = databaseFile.Read(header)
	if err != nil {
		return 0, 0, err
	}

	var pageSize uint16
	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		return 0, 0, fmt.Errorf("failed to read integer: %w", err)
	}
	var numberOfTable uint16
	if err := binary.Read(bytes.NewReader(header[103:105]), binary.BigEndian, &numberOfTable); err != nil {
		return 0, 0, fmt.Errorf("failed to read numberOfTable: %w", err)
	}
	return pageSize, numberOfTable, nil
}
