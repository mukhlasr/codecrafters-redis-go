package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

type command struct {
	cmd  string
	args []string
}

func parseCommand(r *bufio.Reader) (command, int, error) {
	var cmd command

	msg, n, err := parseMessage(r)
	if err != nil {
		return cmd, n, fmt.Errorf("failed to parse message: %w", err)
	}

	if msg.Type != "array" {
		return cmd, n, errors.New("expected array message")
	}

	arr, ok := msg.Content.([]message)
	if !ok {
		return cmd, n, errors.New("invalid array message")
	}

	if len(arr) < 1 {
		return cmd, n, errors.New("empty array")
	}

	cmd.cmd, ok = arr[0].Content.(string)
	if !ok {
		return cmd, n, errors.New("invalid command")
	}

	for i := 1; i < len(arr); i++ {
		arg, ok := arr[i].Content.(string)
		if !ok {
			return cmd, n, errors.New("invalid argument")
		}

		cmd.args = append(cmd.args, arg)
	}

	return cmd, n, nil
}

type message struct {
	Type    string
	Content any
}

func parseMessage(r *bufio.Reader) (message, int, error) {
	readLength := func(b *bufio.Reader) (int, int, error) {
		lengthStr, n, err := readUntilCRLF(b)
		if err != nil {
			return -1, n, nil
		}

		length, err := strconv.Atoi(string(lengthStr))
		return length, n, err
	}

	numBytesRead := 0

	b, err := r.ReadByte()
	if err != nil {
		return message{}, numBytesRead, err
	}

	numBytesRead++

	switch b {
	case '*': // array
		length, n, err := readLength(r)
		if err != nil {
			return message{}, numBytesRead, fmt.Errorf("failed getting array length: %w", err)
		}
		numBytesRead += n

		arr := make([]message, length)

		for i := 0; i < length; i++ {
			msg, n, err := parseMessage(r)
			if err != nil {
				return message{}, numBytesRead, fmt.Errorf("failed to parse array element: %w", err)
			}

			arr[i] = msg
			numBytesRead += n
		}

		return message{
			Type:    "array",
			Content: arr,
		}, numBytesRead, nil
	case '$': // bulk string
		length, n, err := readLength(r)
		if err != nil {
			return message{}, numBytesRead, fmt.Errorf("failed getting bulk string length: %w", err)
		}

		numBytesRead += n

		data, n, err := readBulkString(r, length)
		if err != nil {
			return message{}, numBytesRead, fmt.Errorf("failed to read bulkstring: %w", err)
		}

		numBytesRead += n

		return message{
			Type:    "bulkstring",
			Content: data,
		}, numBytesRead, nil
	case '+': // simple string
		line, err := r.ReadBytes('\n')
		if err != nil {
			return message{}, numBytesRead, err
		}

		numBytesRead += len(line)

		if len(line) < 1 {
			return message{}, numBytesRead, errors.New("empty line")
		}

		if !bytes.HasSuffix(line, []byte("\r\n")) {
			return message{}, numBytesRead, errors.New("invalid line ending")
		}

		data := string(line[:len(line)-2]) // remove the CRLF
		return message{
			Type:    "simplestring",
			Content: data,
		}, numBytesRead, nil
	}

	return message{}, numBytesRead, errors.New("unknown message type")
}

func readUntilCRLF(r *bufio.Reader) ([]byte, int, error) {
	n := 0
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, n, err
	}

	n += len(line)

	if len(line) < 1 {
		return nil, n, errors.New("empty line")
	}

	if !bytes.HasSuffix(line, []byte("\r\n")) {
		return nil, n, errors.New("invalid line ending")
	}

	return line[:len(line)-2], n, nil // remove the CRLF
}
