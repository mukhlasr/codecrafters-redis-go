package main

import (
	"log"
	"net"
)

type Replica struct {
	Addr string
	Port int
	Conn net.Conn

	SendingMessageChan chan string
}

func (r *Replica) SendCommand(cmd command) {
	msg := EncodeBulkStrings(append([]string{cmd.cmd}, cmd.args...)...)
	_, err := r.Conn.Write([]byte(msg))
	if err != nil {
		log.Println("Error sending message to replica:", err.Error())
		return
	}
}

func (r *Replica) Close() {
	r.Conn.Close()
}
