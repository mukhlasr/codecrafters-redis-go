package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type message struct {
	cmd  string
	args []string
}

func parseMessage(r *bufio.Reader) (message, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return message{}, err
	}

	if len(b) < 1 {
		return message{}, errors.New("empty line")
	}

	if b[0] != '*' {
		return message{}, errors.New("the first command must be an array")
	}

	readLength := func(b *bufio.Reader) (int, error) {
		line, err := b.ReadBytes('\n')
		if err != nil {
			return -1, err
		}

		if len(line) < 1 {
			return -1, errors.New("empty line")
		}

		if !bytes.HasSuffix(line, []byte("\r\n")) {
			return -1, errors.New("invalid line ending")
		}

		lengthStr := string(line[:len(line)-2]) // remove the CRLF
		return strconv.Atoi(lengthStr)
	}

	lengthBytes := b[1:]
	lengthStr := string(lengthBytes[:len(lengthBytes)-2]) // remove the CRLF
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return message{}, fmt.Errorf("invalid ararys length: %w", err)
	}

	if length < 1 {
		return message{}, errors.New("empty command")
	}

	msg := message{
		args: make([]string, length-1),
	}

	for i := 0; i < length; i++ {
		var data string
		b, err := r.ReadByte()
		if err != nil {
			return message{}, err
		}

		switch b {
		case '$': // bulk string
			length, err := readLength(r)
			if err != nil {
				return message{}, fmt.Errorf("failed getting bulk string length: %w", err)
			}

			data, err = readBulkString(r, length)
			if err != nil {
				return message{}, fmt.Errorf("failed to read bulkstring: %w", err)
			}
		}

		if i == 0 {
			msg.cmd = data
			continue
		}

		msg.args[i-1] = data
	}

	return msg, nil
}
