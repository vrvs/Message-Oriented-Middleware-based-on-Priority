package handler

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"bufio"
	"fmt"
	"log"
	"net"

	convert "github.com/mitchellh/mapstructure"
)

func ServerConsumerHandler() error {
	fmt.Println("Ligando consumer server")
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
	fmt.Println("Escutando consumer")
	marshaller := marshaller.NewMarshaller()

	for {
		// will listen for message to process ending in newline (\n)
		msg, _ := bufio.NewReader(conn).ReadString('\n')

		// process for string received
		msgUnmarshalled := marshaller.Unmarshall([]byte(msg))

		message := models.Message{}
		convert.Decode(msgUnmarshalled, &message)

		switch message.Head {
		case "TopicRegister":
			fmt.Println(message.TopicName)
			// broker.TopicRegister(message.TopicName, message.Conn)
		default:
		}

		// send new string back to client

		// conn.Write([]byte(newmessage + "\n"))
	}
}
