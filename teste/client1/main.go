package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/producer"
	"fmt"
	"net"
)

// producer
func main() {
	conn, _ := net.Dial("tcp", "localhost:5555")
	pub, err := producer.NewPublisher(conn)
	fmt.Println("err:", err)

	for {
		pub.TopicDeclare("Outro", 6)
	}
}
