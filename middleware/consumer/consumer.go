package consumer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"errors"
	"net"
)

var marsh = marshaller.NewMarshaller()

type Subscriber interface {
	Subscribe(topicName string, identifier string)
	Receive(response interface{}) error
}

type subscriber struct {
	handler *consumerHanlder
}

func NewSubscriber(conn net.Conn) (Subscriber, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}
	handler := newConsumerHanlder(conn)

	return &subscriber{
		handler: handler,
	}, nil
}

func (s *subscriber) Subscribe(topicName string, identifier string) {
	msg := models.Message{
		Head:       "Subscribe",
		TopicName:  topicName,
		Identifier: identifier,
	}

	msgMarshalled := marsh.Marshall(msg)

	s.handler.send(msgMarshalled)
}

func (s *subscriber) Receive(object interface{}) error {

	response, err := s.handler.receive()

	if err != nil {
		return err
	}

	res := adapter.ResponseFromJson(response)

	if res.Error != "" {
		return errors.New(res.Error)
	}

	err = marsh.Unmarshall(res.Body, object)

	if err != nil {
		return err
	}

	return nil
}
