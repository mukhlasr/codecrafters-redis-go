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
		log.Println("running replica", r.Addr, r.Port)
		for msg := range r.SendingMessageChan {
			_, _ = r.Conn.Write([]byte(msg))
		}
	}()
}

func (r *Replica) Close() {
	r.Conn.Close()
}
