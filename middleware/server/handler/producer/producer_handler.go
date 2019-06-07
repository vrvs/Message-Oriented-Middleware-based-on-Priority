package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"encoding/json"
	"log"
	"net"
)

type producerHandler struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

var handler producerHandler

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

	handler = producerHandler{
		conn:    conn,
		encoder: jsonEncoder,
		decoder: jsonDecoder,
	}

	listen()

	return nil
}

func listen() {
	marshaller := marshaller.NewMarshaller()

	for {
		msg := receive()

		msgUnmarshalled := marshaller.Unmarshall(msg)
		switch msgUnmarshalled.Head {
		case "TopicDeclare":
			// broker.TopicDeclare(msgUnmarshalled.TopicName, msgUnmarshalled.MaxPriority)
		case "Publish":
			// broker.Publish(msgUnmarshalled.TopicName, msgUnmarshalled.MessagePriority, msgUnmarshalled.Body)
		}

		// adicionar um send para resposta do cliente
	}
}

func receive() []byte {
	var msg []byte
	err := handler.decoder.Decode(&msg)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return msg
}
