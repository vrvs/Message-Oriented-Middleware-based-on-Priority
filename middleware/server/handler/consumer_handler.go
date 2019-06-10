package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"encoding/json"
	"log"
	"net"
)

func ServerConsumerHandler(brokerConsumer *broker.Broker) error {
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

		go handleConsumerRequest(conn, brokerConsumer)
	}

	return nil
}

func handleConsumerRequest(conn net.Conn, brokerConsumer *broker.Broker) error {
	jsonDecoder := json.NewDecoder(conn)

	go brokerConsumer.Broadcast()

	for {
		// will listen for message to process
		var msg []byte
		err := jsonDecoder.Decode(&msg)
		if err != nil {
			return err
		}
		// process for string received
		if msg[0] == '{' {
			message := adapter.MessageFromJson(msg)
			switch message.Head {
			case "Subscribe":
				brokerConsumer.Subscribe(conn, message.TopicName, message.Identifier)
			default:
			}
		} else {
			log.Println("Error: message incomplete")
		}
		// send new string back to client
	}
}
