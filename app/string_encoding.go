package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

func DecodeString(r *bufio.Reader) (string, error) {
	length, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	if length&0b11000000 != 0b11000000 {
		// length-prefixed
		return decodeLengthPrefixed(r, length)
	}

	remainingSixBits := length & 0b00111111

	switch remainingSixBits {
	case 0:
		// next bit is an 8 bit integer string
		i, err := decodeInt(r, 8)
		if err != nil {
			return "", err
		}

		return strconv.Itoa(i), nil
	case 1:
		// next 2 bits are an 16 bit integer string
		i, err := decodeInt(r, 16)
		if err != nil {
			return "", err
		}

		return strconv.Itoa(i), nil
	case 2:
		// next 4 bits are an 32 bit integer string
		i, err := decodeInt(r, 32)
		if err != nil {
			return "", err
		}

		return strconv.Itoa(i), nil
	case 3:
		// LZF compressed string
		return "", errors.New("unimplemented")
	default:
		return decodeLengthPrefixed(r, length)
	}
}

func decodeLengthPrefixed(r *bufio.Reader, length byte) (string, error) {
	b := make([]byte, length)
	n, err := r.Read(b)
	if err != nil {
		return "", err
	}

	// fmt.Println(string(b), n, length)

	if n != int(length) {
		return "", fmt.Errorf("consumed byte is not equal length")
	}

	return string(b), nil
}

func decodeInt(r *bufio.Reader, bitSize int) (int, error) {
	switch bitSize {
	case 8, 16, 32:
	default:
		return 0, errors.New("unknown bitSize")
	}

	switch bitSize {
	case 8:
		var res int8
		if err := binary.Read(r, binary.LittleEndian, &res); err != nil {
			return 0, err
		}

		return int(res), nil
	case 16:
		var res int16
		if err := binary.Read(r, binary.LittleEndian, &res); err != nil {
			return 0, err
		}

		return int(res), nil
	case 32:
		var res int32
		if err := binary.Read(r, binary.LittleEndian, &res); err != nil {
			return 0, err
		}

		return int(res), nil
	}

	return 0, errors.New("unknown bitSize")
}
