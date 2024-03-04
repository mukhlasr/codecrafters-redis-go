package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var defaultCurrentDB = 0

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	s := &Server{
		Config: map[string]string{},
	}

	flag, err := parseFlag(os.Args)
	if err != nil {
		log.Fatalln(err)
	}

	s.Port = flag.port
	s.Config["dir"] = flag.dir
	s.Config["dbfilename"] = flag.dbfilename

	if flag.masterAddr != "" && flag.masterPort != 0 {
		s.IsSlave = true
		s.MasterAddress = flag.masterAddr
		s.MasterPort = flag.masterPort
	}

	fmt.Println(s.IsSlave, s.MasterAddress, s.MasterPort)
	log.Println(s.Run(context.Background()))
}

type Server struct {
	Addr          string
	Port          int
	Config        map[string]string
	IsSlave       bool
	MasterAddress string
	MasterPort    int

	RDB RDB
}

func (s *Server) Run(ctx context.Context) error {
	s.LoadRDB()

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

func (s *Server) LoadRDB() {
	var rdb RDB
	dir := s.Config["dir"]
	filename := s.Config["dbfilename"]
	if dir == "" || filename == "" {
		db := &Database{}
		db.ID = 0
		db.Fields = map[string]Field{}

		rdb.Databases = append(rdb.Databases, db)
	}

	path := filepath.Join(dir, filename)
	_, err := os.Stat(path)

	if err == nil {
		rdb = ParseFile(path)
	}

	s.RDB = rdb
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
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(db.Fields)))
		for k := range db.Fields {
			sb.WriteString(BulkString(k).Encode())
		}
		return sb.String()
	}
	return "*0"
}

func (s *Server) onInfo(args []string) string {
	switch args[0] {
	case "replication":
		if !s.IsSlave {
			return BulkString("role:master").Encode()
		}
		return BulkString("role:slave").Encode()
	}

	return "$-1\r\n"
}
