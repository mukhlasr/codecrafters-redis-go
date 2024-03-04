package main

import (
	"bufio"
	"errors"
)

func DecodeLength(r *bufio.Reader) (int, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	var res int
	switch {
	case b|0b00111111 == 0b00111111:
		res = int(b) & 0b00111111
		return res, nil
	case b&0b01000000 == 0b01000000:
		res = int(b) & 0b00111111
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		res <<= 8
		res |= int(b)
		return res, nil
	case b&0b10000000 == 0b10000000:
		bs := make([]byte, 4)
		_, err := r.Read(bs)
		if err != nil {
			return 0, err
		}

		res |= int(bs[0])
		for i := 1; i < len(bs); i++ {
			res <<= 8
			res |= int(bs[i])
		}

		return res, nil

	default:
		return 0, errors.New("unknown encoding")
	}
}
