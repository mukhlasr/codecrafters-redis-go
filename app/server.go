package main

import (
	"bufio"
	"context"
	"encoding/base64"
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

var (
	defaultCurrentDB = 0
	emptyRDBB64      = `UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==`
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	s := &Server{
		Config:        map[string]string{},
		ReplicationID: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
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

	log.Println(s.Run(context.Background()))
}

type Server struct {
	Addr              string
	Port              int
	Config            map[string]string
	ReplicationID     string
	ReplicationOffset int
	IsSlave           bool
	MasterAddress     string
	MasterPort        int

	MasterConn   net.Conn
	ReplicasConn []net.Conn

	RDB RDB
}

func (s *Server) Run(ctx context.Context) error {
	s.LoadRDB()

	if s.IsSlave {
		conn, err := s.connectToMaster()
		if err != nil {
			return err
		}

		s.MasterConn = conn
		go func() {
			defer s.MasterConn.Close()
			err := s.HandleMaster()
			if err != nil {
				log.Println(err)
			}
		}()
	}

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

func (s *Server) connectToMaster() (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", s.MasterAddress, s.MasterPort))
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(conn)

	_, err = conn.Write([]byte(EncodeBulkStrings("ping")))
	if err != nil {
		conn.Close()
		return nil, err
	}

	msg, err := parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}
	log.Printf("%+v\n", msg)

	_, err = conn.Write([]byte(EncodeBulkStrings("replconf", "listening-port", strconv.Itoa(s.Port))))
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = conn.Write([]byte(EncodeBulkStrings("replconf", "capa", "psync2")))
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = conn.Write([]byte(EncodeBulkStrings("psync", "?", "-1")))
	if err != nil {
		conn.Close()
		return nil, err
	}

	_, err = parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (s *Server) HandleMaster() error {
	r := bufio.NewReader(s.MasterConn)
	for {
		msg, err := parseMessage(r)
		if err != nil {
			return err
		}

		if msg.Type != "array" {
			return errors.New("expected array message")
		}

		arr, ok := msg.Content.([]message)
		if !ok {
			return errors.New("invalid array message")
		}

		if len(arr) < 1 {
			return errors.New("empty array")
		}

		cmd, ok := arr[0].Content.(string)
		if !ok {
			return errors.New("invalid command")
		}

		switch cmd {
		case "psync":

		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		r := bufio.NewReader(conn)
		msg, err := parseCommand(r)
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

func (s *Server) runMessage(conn net.Conn, c command) error {
	var resp string
	switch cmd := strings.ToLower(c.cmd); cmd {
	case "ping":
		resp = "+PONG\r\n"
	case "echo":
		resp = fmt.Sprintf("+%v\r\n", c.args[0])
	case "set":
		log.Println("setting ", c.args)
		resp = s.onSet(c.args)
		s.propagateCmdToReplicas(c)
	case "get":
		resp = s.onGet(c.args)
	case "config":
		resp = s.onConfig(c.args)
	case "keys":
		resp = s.onKeys(c.args)
	case "info":
		resp = s.onInfo(c.args)
	case "replconf":
		resp = s.onReplConf(c.args)
	case "psync":
		resp = s.onPsync(c.args)
		s.ReplicasConn = append(s.ReplicasConn, conn)
	default:
		return fmt.Errorf("unknown command")
	}

	_, err := conn.Write([]byte(resp))
	return err
}

func (s *Server) propagateCmdToReplicas(cmd command) {
	for i, replica := range s.ReplicasConn {
		log.Println("writing to replica", i, cmd)
		_, err := replica.Write([]byte(EncodeBulkStrings(append([]string{cmd.cmd}, cmd.args...)...)))
		if err != nil {
			log.Println(err)
		}
	}
}

func (s *Server) onSet(args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'set' command\r\n"
	}

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

	return EncodeBulkStrings(key, val)
}

func (s *Server) onKeys(args []string) string {
	db := s.RDB.Databases[defaultCurrentDB]
	switch args[0] {
	case "*":
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(db.Fields)))
		for k := range db.Fields {
			sb.WriteString(EncodeBulkString(k))
		}
		return sb.String()
	}
	return "*0"
}

func (s *Server) onInfo(args []string) string {
	switch args[0] {
	case "replication":
		if s.IsSlave {
			return EncodeBulkString("role:slave")
		}

		return EncodeBulkString("role:master" + "\r\n" +
			fmt.Sprintf("master_replid:%s", s.ReplicationID) + "\r\n" +
			fmt.Sprintf("master_repl_offset:%d", s.ReplicationOffset))

	}

	return "$-1\r\n"
}

func (s *Server) onReplConf(args []string) string {
	return "+OK\r\n"
}

func (s *Server) onPsync(args []string) string {
	emptyRDB, err := base64.StdEncoding.DecodeString(emptyRDBB64)
	if err != nil {
		return "-ERR failed to parse empty RDB"
	}

	encodedEmptyRDB := fmt.Sprintf("$%d\r\n%s", len(emptyRDB), emptyRDB)
	return fmt.Sprintf("+FULLRESYNC %s %d\r\n", s.ReplicationID, s.ReplicationOffset) + encodedEmptyRDB
}
