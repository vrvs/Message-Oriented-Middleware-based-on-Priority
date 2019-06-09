package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"Message-Oriented-Middleware-based-on-Priority/middleware/producer"
	"net"
	"time"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:5555")
	p, _ := producer.NewPublisher(conn)
	p.TopicDeclare("test", 8)
	for {
		v := producer.Publishing{
			Priority: 8,
			Body:     models.Message{TopicName: "heyyyyy"},
		}
		p.Publish("test", v)
		v = producer.Publishing{
			Priority: 9,
			Body:     models.Message{TopicName: "you"},
		}
		p.Publish("test", v)
		v = producer.Publishing{
			Priority: 10,
			Body:     models.Message{TopicName: "boy"},
		}
		p.Publish("test", v)
		time.Sleep(15000000000)
	}
}
