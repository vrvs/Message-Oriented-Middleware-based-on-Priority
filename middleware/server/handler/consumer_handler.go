package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"encoding/json"
	"log"
	"net"
)

var brokerConsumer = broker.NewBroker()

func ServerConsumerHandler() error {
	log.Println("Starting consumer server")
	ln, err := net.Listen("tcp", "localhost:5556")
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting request:", err)
		}

		go handleConsumerRequest(conn)
	}

	return nil
}

func handleConsumerRequest(conn net.Conn) {
	jsonDecoder := json.NewDecoder(conn)
	var msg []byte

	for {
		// will listen for message to process
		jsonDecoder.Decode(&msg)

		// process for string received
		if msg[0] == '{' {
			message := adapter.MessageFromJson(msg)
			switch message.Head {
			case "Subscribe":
				brokerConsumer.Subscribe(message.Conn, message.TopicName)
			default:
			}
		} else {
			log.Println("Error: message incomplete")
		}
		// send new string back to client
	}
}
