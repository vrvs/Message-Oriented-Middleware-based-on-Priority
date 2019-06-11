package producer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
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
	handler *producerHanlder
}

func NewPublisher(conn net.Conn) (Publisher, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	handler := newProducerHanlder(conn)

	return &publisher{
		handler: handler,
	}, nil
}

func (p *publisher) TopicDeclare(topicName string, maxPriority int) {
	msg := models.Message{
		Head:        "TopicDeclare",
		TopicName:   topicName,
		MaxPriority: maxPriority,
	}

	msgMarshalled := marsh.Marshall(msg)

	p.handler.send(msgMarshalled)
}

func (p *publisher) Publish(topicName string, content Publishing) {
	msg := models.Message{
		Head:            "Publish",
		TopicName:       topicName,
		MessagePriority: content.Priority,
		Body:            marsh.Marshall(content.Body),
	}

	msgMarshalled := marsh.Marshall(msg)

	p.handler.send(msgMarshalled)
}

func (p *publisher) receive() error {
	var response []byte

	response, err := p.handler.receive()

	if err != nil {
		return err
	}

	res := adapter.ResponseFromJson(response)

	if res.Error != "" {
		return errors.New(res.Error)
	}

	return nil
}
