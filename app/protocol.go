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

func parseCommand(r *bufio.Reader) (command, error) {
	var cmd command

	msg, err := parseMessage(r)
	if err != nil {
		return cmd, fmt.Errorf("failed to parse message: %w", err)
	}

	if msg.Type != "array" {
		return cmd, errors.New("expected array message")
	}

	arr, ok := msg.Content.([]message)
	if !ok {
		return cmd, errors.New("invalid array message")
	}

	if len(arr) < 1 {
		return cmd, errors.New("empty array")
	}

	cmd.cmd, ok = arr[0].Content.(string)
	if !ok {
		return cmd, errors.New("invalid command")
	}

	for i := 1; i < len(arr); i++ {
		arg, ok := arr[i].Content.(string)
		if !ok {
			return cmd, errors.New("invalid argument")
		}

		cmd.args = append(cmd.args, arg)
	}

	return cmd, nil
}

type message struct {
	Type    string
	Content any
}

func parseMessage(r *bufio.Reader) (message, error) {
	readLength := func(b *bufio.Reader) (int, error) {
		lengthStr, err := readUntilCRLF(b)
		if err != nil {
			return -1, nil
		}

		return strconv.Atoi(string(lengthStr))
	}

	b, err := r.ReadByte()
	if err != nil {
		return message{}, err
	}

	switch b {
	case '*': // array
		length, err := readLength(r)
		if err != nil {
			return message{}, fmt.Errorf("failed getting array length: %w", err)
		}

		arr := make([]message, length)

		for i := 0; i < length; i++ {
			msg, err := parseMessage(r)
			if err != nil {
				return message{}, fmt.Errorf("failed to parse array element: %w", err)
			}

			arr[i] = msg
		}

		return message{
			Type:    "array",
			Content: arr,
		}, nil
	case '$': // bulk string
		length, err := readLength(r)
		if err != nil {
			return message{}, fmt.Errorf("failed getting bulk string length: %w", err)
		}

		data, err := readBulkString(r, length)
		if err != nil {
			return message{}, fmt.Errorf("failed to read bulkstring: %w", err)
		}

		return message{
			Type:    "bulkstring",
			Content: data,
		}, nil
	case '+': // simple string
		line, err := r.ReadBytes('\n')
		if err != nil {
			return message{}, err
		}

		if len(line) < 1 {
			return message{}, errors.New("empty line")
		}

		if !bytes.HasSuffix(line, []byte("\r\n")) {
			return message{}, errors.New("invalid line ending")
		}

		data := string(line[:len(line)-2]) // remove the CRLF
		return message{
			Type:    "simplestring",
			Content: data,
		}, nil
	}

	return message{}, errors.New("unknown message type")
}

func readUntilCRLF(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 1 {
		return nil, errors.New("empty line")
	}

	if !bytes.HasSuffix(line, []byte("\r\n")) {
		return nil, errors.New("invalid line ending")
	}

	return line[:len(line)-2], nil // remove the CRLF
}
