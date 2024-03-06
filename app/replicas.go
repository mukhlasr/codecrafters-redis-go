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
	r.SendingMessageChan <- EncodeBulkStrings(append([]string{cmd.cmd}, cmd.args...)...)
}

func (r *Replica) Run() {
	go func() {
		for msg := range r.SendingMessageChan {
			log.Println("sending", msg)
			_, _ = r.Conn.Write([]byte(msg))
		}
	}()
}

func (r *Replica) Close() {
	r.Conn.Close()
}