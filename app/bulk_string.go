package main

import (
	"bufio"
	"fmt"
	"strings"
)

func EncodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func EncodeBulkStrings(ss ...string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(ss)))
	for _, s := range ss {
		sb.WriteString(EncodeBulkString(s))
	}

	return sb.String()
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
