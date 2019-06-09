package producer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"errors"
	"net"
)

var marsh = marshaller.NewMarshaller()

type Publishing struct {
	Priority int
	Body     interface{}
}

type Publisher interface {
	TopicDeclare(topicName string, maxPriority int)
	Publish(queueName string, content Publishing)
}

type publisher struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
	jsonDecoder *json.Decoder
}

func NewPublisher(conn net.Conn) (Publisher, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return &publisher{
		conn:        conn,
		jsonEncoder: jsonEncoder,
		jsonDecoder: jsonDecoder,
	}, nil
}

func (p *publisher) TopicDeclare(topicName string, maxPriority int) {
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

	return nil
}

func (p *publisher) receive() error {
	var response []byte

	p.jsonDecoder.Decode(&response)

	res := adapter.ResponseFromJson(response)

	if res.Error != "" {
		return errors.New(res.Error)
	}

	return nil
}
