package producer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"errors"
	"fmt"
	"net"
	"time"
)

var marsh = marshaller.NewMarshaller()

type Publishing struct {
	Priority int64
	Body     interface{}
}

type Publisher interface {
	TopicDeclare(topicName string, maxPriority int64)
	Publish(queueName string, content Publishing)
}

type publisher struct {
	conn net.Conn
}

func NewPublisher(conn net.Conn) (Publisher, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	return &publisher{
		conn: conn,
	}, nil
}

func (p *publisher) TopicDeclare(topicName string, maxPriority int64) {
	msg := models.Message{
		Head:        "TopicDeclare",
		TopicName:   topicName,
		MaxPriority: maxPriority,
	}

	p.send(msg)
}

func (p *publisher) Publish(topicName string, content Publishing) {
	msg := models.Message{
		Head:            "Publish",
		TopicName:       topicName,
		MessagePriority: content.Priority,
		Body:            marsh.Marshall(content.Body),
	}

	p.send(msg)
}

func (p *publisher) send(msg models.Message) error {
	msgMarshalled := marsh.Marshall(msg)
	message := string(msgMarshalled)
	time.Sleep(5000)
	fmt.Fprintf(p.conn, message+"\n")

	return nil
}

// listen for reply
//message, _ := bufio.NewReader(conn).ReadString('\n')
