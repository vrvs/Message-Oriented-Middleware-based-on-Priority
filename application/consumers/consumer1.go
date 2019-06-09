package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/consumer"
	"fmt"
	"net"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:5556")
	p, _ := consumer.NewSubscriber(conn)
	p.Subscribe("test")
	for {
		v, _ := p.Receive()
		s := string(*v)
		fmt.Println(s)
	}
}
