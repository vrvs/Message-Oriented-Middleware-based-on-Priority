package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"Message-Oriented-Middleware-based-on-Priority/middleware/producer"
	"net"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:5555")
	p, _ := producer.NewPublisher(conn)
	p.TopicDeclare("test", 8)
	for {
		v := producer.Publishing{
			Priority: 9,
			Body:     models.Message{TopicName: "heyyyyy"},
		}
		p.Publish("test", v)
	}
}
