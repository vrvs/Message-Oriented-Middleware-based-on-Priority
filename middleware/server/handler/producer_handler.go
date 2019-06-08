package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"log"
	"net"
)

type producerHandler struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

var prodHandler producerHandler

func NewProducerHandler() error {
	ln, err := net.Listen("tcp", "localhost:5555")
	if err != nil {
		return err
	}

	conn, err := ln.Accept()
	if err != nil {
		return err
	}

	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	prodHandler = producerHandler{
		conn:    conn,
		encoder: jsonEncoder,
		decoder: jsonDecoder,
	}

	listenl()

	return nil
}

func listenl() {
	marshaller := marshaller.NewMarshaller()

	for {
		msg := receive()

		msgUnmarshalled := marshaller.Unmarshall(msg)
		message := msgUnmarshalled.(models.Message)
		switch message.Head {
		case "TopicDeclare":
			// broker.TopicDeclare(message.TopicName, message.MaxPriority)
		case "Publish":
			// broker.Publish(message.TopicName, message.MessagePriority, message.Body)
		}
	}
}

func receive() []byte {
	var msg []byte
	err := prodHandler.decoder.Decode(&msg)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return msg
}
