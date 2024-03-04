package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var defaultCurrentDB = 0

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	s := &Server{
		RDB: RDB{
			Databases: []*Database{
				0: {
					ID:     0,
					Keys:   []string{},
					Fields: map[string]Field{},
				},
			},
		},
		Port: 6379,
	}

	log.Println(s.Run(context.Background()))
}

type Server struct {
	Addr string
	Port int

	RDB    RDB
	Config map[string]string
}

func (s *Server) Run(ctx context.Context) error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.Addr, s.Port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %d: %w", s.Port, err)
	}
	defer l.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := l.Accept()
			if err != nil {
				return fmt.Errorf("error accepting connection: %w", err)
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		r := bufio.NewReader(conn)
		msg, err := parseMessage(r)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			fmt.Println("Error reading message:", err.Error())
			return
		}

		err = s.runMessage(conn, msg)
		if err != nil {
			fmt.Println("Error running message:", err.Error())
			return
		}
	}
}

func (s *Server) runMessage(conn net.Conn, m message) error {
	var resp string
	switch cmd := strings.ToLower(m.cmd); cmd {
	case "ping":
		resp = "+PONG\r\n"
	case "echo":
		resp = fmt.Sprintf("+%v\r\n", m.args[0])
	case "set":
		resp = s.onSet(m.args)
	case "get":
		resp = s.onGet(m.args)
	case "config":
		resp = s.onConfig(m.args)
	case "keys":
		resp = s.onKeys(m.args)
	case "info":
		resp = s.onInfo(m.args)
	default:
		return fmt.Errorf("unknown command")
	}

	_, err := conn.Write([]byte(resp))
	return err
}

func (s *Server) onSet(args []string) string {
	database := s.RDB.Databases[defaultCurrentDB]
	key := args[0]
	val := args[1]
	database.Set(key, val)

	if len(args) == 4 {
		ttl, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			<-time.After(time.Duration(ttl) * time.Millisecond)
			database.Unset(key)
		}()
	}
	return "+OK\r\n"
}

func (s *Server) onGet(args []string) string {
	data, ok := s.RDB.Databases[defaultCurrentDB].Get(args[0])

	if !ok {
		return "$-1\r\n"
	}

	return fmt.Sprintf("+%v\r\n", data)
}

func (s *Server) onConfig(args []string) string {
	key := args[1]
	val := s.Config[key]

	if len(val) == 0 {
		return "$-1\r\n"
	}

	var sb strings.Builder
	sb.WriteString("*2\r\n")
	sb.WriteString(BulkString(key).Encode())
	sb.WriteString(BulkString(val).Encode())
	return sb.String()
}

func (s *Server) onKeys(args []string) string {
	db := s.RDB.Databases[defaultCurrentDB]
	switch args[0] {
	case "*":
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(db.Keys)))
		for _, k := range db.Keys {
			sb.WriteString(BulkString(k).Encode())
		}
		return sb.String()
	}
	return "*0"
}

func (*Server) onInfo(args []string) string {
	switch args[0] {
	case "replication":
		return BulkString("role:master").Encode()
	}

	return "$-1\r\n"
}
