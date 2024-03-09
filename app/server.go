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
	"sync"
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

	MasterConn     net.Conn
	Replicas       []*Replica
	ReplicasMapMux sync.Mutex

	RDB RDB
}

func (s *Server) Run(ctx context.Context) error {
	s.LoadRDB()
	if s.IsSlave {
		conn, err := s.connectToMaster()
		if err != nil {
			return err
		}
		log.Println("connected to master")

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
	dir := s.Config["dir"]
	filename := s.Config["dbfilename"]
	path := filepath.Join(dir, filename)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		db := &Database{}
		db.ID = 0
		db.Fields = map[string]Field{}

		s.RDB.Databases = append(s.RDB.Databases, db)

		return
	}

	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	rdb, err := ParseFile(bufio.NewReader(file))
	if err != nil {
		log.Fatal(err)
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

	_, err = parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

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

	msg, err := parseMessage(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	if msg.Type != "simplestring" {
		conn.Close()
		return nil, errors.New("expected simplestring message")
	}

	msgContents := strings.Split(msg.Content.(string), " ")
	if len(msgContents) < 3 {
		conn.Close()
		return nil, errors.New("invalid fullresync message")
	}

	_, err = readUntilCRLF(r) // read the $<length>\r\n
	if err != nil {
		conn.Close()
		return nil, err
	}

	rdb, err := ParseFile(r)
	if err != nil {
		conn.Close()
		return nil, err
	}

	s.RDB = rdb

	return conn, nil
}

func (s *Server) HandleMaster() error {
	r := bufio.NewReader(s.MasterConn)
	log.Println("waiting for command from master")

	for {
		cmd, err := parseCommand(r)
		if err != nil {
			return err
		}

		log.Println("received command from master", cmd.cmd, cmd.args)

		switch strings.ToLower(cmd.cmd) {
		case "set":
			_ = s.onSet(cmd.args) // do not send back respond to master
		case "replconf":
			log.Println("received replconf command from master")
			str := s.onSlaveReplConf(cmd.args)
			_, err = s.MasterConn.Write([]byte(str))
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		cmd, err := parseCommand(r)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			fmt.Println("Error reading message:", err.Error())
			return
		}

		err = s.runCommand(conn, cmd)
		if err != nil {
			fmt.Println("Error running message:", err.Error())
			return
		}
	}
}

func (s *Server) runCommand(conn net.Conn, c command) error {
	var resp string
	switch cmd := strings.ToLower(c.cmd); cmd {
	case "ping":
		resp = "+PONG\r\n"
	case "echo":
		resp = fmt.Sprintf("+%v\r\n", c.args[0])
	case "set":
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
		resp = s.onMasterReplConf(conn, c.args)
	case "psync":
		resp = s.onPsync(c.args)
	default:
		return fmt.Errorf("unknown command")
	}

	_, err := conn.Write([]byte(resp))
	return err
}

func (s *Server) addReplica(conn net.Conn, port int) {
	log.Println("adding replica")
	replica := &Replica{
		Addr: conn.RemoteAddr().String(),
		Port: port,
		Conn: conn,
	}

	s.Replicas = append(s.Replicas, replica)
}

func (s *Server) propagateCmdToReplicas(cmd command) {
	for _, replica := range s.Replicas {
		replica := replica
		replica.SendCommand(cmd)
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

func (s *Server) onMasterReplConf(conn net.Conn, args []string) string {
	switch args[0] {
	case "listening-port":
		if len(args[1:]) < 1 {
			return "-ERR wrong number of arguments for 'replconf' listening-port command\r\n"
		}

		port, err := strconv.Atoi(args[1])
		if err != nil {
			return "-ERR invalid port number\r\n"
		}

		s.addReplica(conn, port)
		return "+OK\r\n"
	}

	return "+OK\r\n"
}

func (s *Server) onSlaveReplConf(args []string) string {
	switch strings.ToLower(args[0]) {
	case "getack":
		return "+REPLCONF ACK 0"
	}

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
