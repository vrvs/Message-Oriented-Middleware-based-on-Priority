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

func ServerProducerHandler() error {
	fmt.Println("Ligando producer server")
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
	fmt.Println("Escutando producer")
	marshaller := marshaller.NewMarshaller()

	for {
		// will listen for message to process ending in newline (\n)

		msg, _ := bufio.NewReader(conn).ReadString('\n')
		// process for string received
		if msg[0] == '{' {
			msgUnmarshalled := marshaller.Unmarshall([]byte(msg))
			message := models.Message{}
			convert.Decode(msgUnmarshalled, &message)
			switch message.Head {
			case "TopicDeclare":
				fmt.Println(message.TopicName, message.MaxPriority)
				// broker.TopicDeclare(message.TopicName, message.MaxPriority)
			case "Publish":
				// broker.Publish(message.TopicName, message.MessagePriority, message.Body)
			}
		} else {

			fmt.Println("falhou")
			fmt.Println(msg)
		}

		// send new string back to client

		// conn.Write([]byte(newmessage + "\n"))
	}
}
