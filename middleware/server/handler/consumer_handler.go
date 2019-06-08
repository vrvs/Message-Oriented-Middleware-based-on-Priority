package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"log"
	"net"
)

type consumerHandler struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

func NewConsumerHandler() (*consumerHandler, error) {
	ln, err := net.Listen("tcp", "localhost:5556")
	if err != nil {
		return nil, err
	}

	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}

	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return &consumerHandler{
		conn:    conn,
		encoder: jsonEncoder,
		decoder: jsonDecoder,
	}, nil
}

func (c *consumerHandler) Listen() {
	marshaller := marshaller.NewMarshaller()

	for {
		msg := c.receive()
		msgUnmarshalled := marshaller.Unmarshall(msg)
		message := msgUnmarshalled.(models.Message)
		switch message.Head {
		case "TopicRegister":
			// broker.TopicRegister(message.TopicName, message.Conn)
		default:
		}
	}
}

func (c *consumerHandler) receive() []byte {
	var msg []byte
	err := prodHandler.decoder.Decode(&msg)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return msg
}
