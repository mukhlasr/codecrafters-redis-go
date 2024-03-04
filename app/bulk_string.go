package main

import (
	"bufio"
	"fmt"
)

type BulkString string

func (s BulkString) Encode() string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func readBulkString(r *bufio.Reader, length int) (string, error) {
	buf := make([]byte, length)

	if _, err := r.Read(buf); err != nil {
		return "", err
	}

	// read the trailing \r\n
	if _, err := r.Read(make([]byte, 2)); err != nil {
		return "", err
	}

	return string(buf), nil
}
