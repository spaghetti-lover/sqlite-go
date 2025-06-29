package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

func printDbInfo(databaseFilePath string) {
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
}
