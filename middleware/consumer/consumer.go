package consumer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"errors"
	"net"
)

var marsh = marshaller.NewMarshaller()

type Subscriber interface {
	Subscribe(topicName string, conn net.Conn) error
	Receive() ([]byte, error)
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

	return &subscriber{
		conn:    conn,
		encoder: jsonEncoder,
		decoder: jsonDecoder,
	}, nil
}

func (s *subscriber) Subscribe(topicName string, conn net.Conn) error {
	if conn == nil {
		return errors.New("error: empty conn")
	}

	msg := models.Message{
		Head:      "Subscribe",
		TopicName: topicName,
		Conn:      conn,
	}

	go s.send(msg)

	return nil
}

func (s *subscriber) Receive() ([]byte, error) {
	msg, err := s.receive()

	if err != nil {
		return make([]byte, 0), err
	}

	return *msg, nil
}

func (s *subscriber) send(msg models.Message) error {
	msgMarshalled := marsh.Marshall(msg)

	err := s.encoder.Encode(msgMarshalled)

	if err != nil {
		return err
	}

	return nil
}

func (s *subscriber) receive() (*[]byte, error) {
	var msg []byte
	err := s.decoder.Decode(&msg)

	if err != nil {
		return nil, err
	}

	msgUnmarshalled := marsh.Unmarshall(msg)

	ans := msgUnmarshalled.(models.Subscribing)

	return &ans.Body, nil
}
