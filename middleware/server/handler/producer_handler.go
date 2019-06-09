package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/adapter"
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

func ServerProducerHandler(brokerPoducer *broker.Broker) error {

	log.Println("Starting producer server")
	fmt.Println(brokerPoducer)
	ln, err := net.Listen("tcp", "localhost:5555")
	if err != nil {
		return err
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting request:", err)
		}

		go handleProducerRequest(conn, brokerPoducer)
	}

	return nil
}

func handleProducerRequest(conn net.Conn, brokerPoducer *broker.Broker) {
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
