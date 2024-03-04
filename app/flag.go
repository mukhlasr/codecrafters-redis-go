package main

import (
	"errors"
	"strconv"
)

type flag struct {
	dir        string
	dbfilename string
	port       int
	masterAddr string
	masterPort int
}

func parseFlag(args []string) (flag, error) {
	flag := flag{
		port: 6379, // default value
	}
	n := len(args)
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--dir":
			i++
			if n-i < 1 {
				return flag, errors.New("empty dir")
			}

			flag.dir = args[i]

		case "--dbfilename":
			i++
			if n-i < 1 {
				return flag, errors.New("empty dbfilename")
			}

			flag.dbfilename = args[i]

		case "--port":
			i++
			if n-i < 1 {
				return flag, errors.New("empty port")
			}

			port, err := strconv.Atoi(args[i])
			if err != nil {
				return flag, errors.New("invalid port")
			}

			flag.port = port

		case "--replicaof":
			i++
			if n-i < 2 {
				return flag, errors.New("invalid --replicaof")
			}

			addr := args[i]
			i++

			port, err := strconv.Atoi(args[i])
			if err != nil {
				return flag, errors.New("invalid master port")
			}

			flag.masterAddr = addr
			flag.masterPort = port
		}
	}

	return flag, nil
}
