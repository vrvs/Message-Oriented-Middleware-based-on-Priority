package proxy

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"errors"
	"net"
	"log"
)

var marsh = marshaller.NewMarshaller()

type Subscribing struct {
	Body     interface{}
}

type Subscriber interface {
	QueueRegister(queueName string) error
	Subscribe() (Subscribing, error)
}

type subscriber struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

func NewSubscriber(conn net.Conn) (Subscriber, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return subscriber{
		conn:    conn,
		encoder: jsonEncoder,
		decoder: jsonDecoder,
	}, nil
}

func (s subscriber) QueueRegister(queueName string) error {
	msg := models.Message{
		Head:        "QueueRegister",
		QueueName:   queueName,
	}

	return s.send(msg)
}

func (s subscriber) Subscribe() (Subscribing, error) {
	msg := s.listen()

	messageUnmarshalled := marsh.Unmarshall(msg)
	
	return Subscribing {
		Body: messageUnmarshalled.Body,
	}, nil
}

func (s subscriber) send(msg models.Message) error {
	msgMarshalled := marsh.Marshall(msg)

	err := s.encoder.Encode(msgMarshalled)

	if err != nil {
		return err
	}

	return nil
}

func (s subscriber) listen() []byte {
	var msg []byte
	err := s.decoder.Decode(&msg)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return msg
}
