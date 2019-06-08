package producer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"errors"
	"net"
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
	conn        net.Conn
	jsonEncoder *json.Encoder
}

func NewPublisher(conn net.Conn) (Publisher, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	jsonEncoder := json.NewEncoder(conn)
	return &publisher{
		conn:        conn,
		jsonEncoder: jsonEncoder,
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
	p.jsonEncoder.Encode(msgMarshalled)
	//fmt.Fprintf(p.conn, message+"\n")
	return nil
}

// listen for reply
//message, _ := bufio.NewReader(conn).ReadString('\n')
