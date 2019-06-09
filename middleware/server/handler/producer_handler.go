package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	b "Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"encoding/json"
	"log"
	"net"
)

var brokerPoducer = b.NewBroker()

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
				brokerPoducer.CreateTopic(message.TopicName, message.MaxPriority)
			case "Publish":
				brokerPoducer.Publish(message.TopicName, message.MessagePriority, message.Body)
			}
		} else {
			log.Println("Error: message incomplete")
		}
		// send new string back to client
	}
}
