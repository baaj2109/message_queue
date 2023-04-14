package main

import (
	"fmt"
	"message_queue/broker"
	"net"
)

func main() {

	listen, err := net.Listen("tcp", "127.0.0.1:1234")
	if err != nil {
		fmt.Println("listen failed, err:", err)
		return
	}
	go broker.Save()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("accept failed , err:", err)
		}
		go broker.Process(conn)
	}

}
