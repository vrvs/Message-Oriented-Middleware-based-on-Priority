package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/consumer"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"fmt"
	"net"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:5556")
	p, _ := consumer.NewSubscriber(conn)
	p.Subscribe("test")
	for {
		v, _ := p.Receive()
		var t models.Message
		json.Unmarshal(v, &t)
		fmt.Println(t)
	}
}
