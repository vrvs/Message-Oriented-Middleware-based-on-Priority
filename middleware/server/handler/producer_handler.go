package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"encoding/json"
	"log"
	"net"
)

func ServerProducerHandler() error {
	log.Println("Starting producer server")
	ln, err := net.Listen("tcp", "localhost:5555")
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting request:", err)
		}

		go handleProducerRequest(conn)
	}

	return nil
}

func handleProducerRequest(conn net.Conn) {
	jsonDecoder := json.NewDecoder(conn)
	var msg []byte

	for {
		// will listen for message to process
		jsonDecoder.Decode(&msg)

		// process for string received
		if msg[0] == '{' {
			message := adapter.MessageFromJson(msg)
			switch message.Head {
			case "TopicDeclare":
				// broker.TopicDeclare(message.TopicName, message.MaxPriority)
			case "Publish":
				// broker.Publish(message.TopicName, message.MessagePriority, message.Body)
			}
		} else {
			log.Println("Error: message incomplete")
		}
		// send new string back to client
	}
}
