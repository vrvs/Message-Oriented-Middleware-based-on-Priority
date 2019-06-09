package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"encoding/json"
	"fmt"
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

func handleConsumerRequest(conn net.Conn, brokerConsumer *broker.Broker) {
	jsonDecoder := json.NewDecoder(conn)
	var msg []byte

	go brokerConsumer.Broadcast()

	for {
		// will listen for message to process
		jsonDecoder.Decode(&msg)

		// process for string received
		if msg[0] == '{' {
			message := adapter.MessageFromJson(msg)
			fmt.Println(message.Conn)
			switch message.Head {
			case "Subscribe":
				brokerConsumer.Subscribe(conn, message.TopicName)
			default:
			}
		} else {
			log.Println("Error: message incomplete")
		}
		// send new string back to client
	}
}
