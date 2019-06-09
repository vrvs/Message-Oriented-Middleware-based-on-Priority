package consumer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

var marsh = marshaller.NewMarshaller()

type Subscriber interface {
	Subscribe(topicName string)
	Receive() (*[]byte, error)
}

type subscriber struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
	jsonDecoder *json.Decoder
}

func NewSubscriber(conn net.Conn) (Subscriber, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return &subscriber{
		conn:        conn,
		jsonEncoder: jsonEncoder,
		jsonDecoder: jsonDecoder,
	}, nil
}

func (s *subscriber) Subscribe(topicName string) {
	msg := models.Message{
		Head:      "Subscribe",
		TopicName: topicName,
		Conn:      s.conn,
	}

	s.send(msg)
}

func (s *subscriber) Receive() (*[]byte, error) {
	var response []byte

	s.jsonDecoder.Decode(&response)

	fmt.Println(string(response))

	res := adapter.ResponseFromJson(response)

	if res.Error != "" {
		return nil, errors.New(res.Error)
	}

	return &res.Body, nil
}

func (s *subscriber) send(msg models.Message) {
	msgMarshalled := marsh.Marshall(msg)
	s.jsonEncoder.Encode(msgMarshalled)
}
