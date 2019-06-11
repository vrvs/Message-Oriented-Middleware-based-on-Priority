package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/consumer"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/momp-app/models"
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

var aggData map[string]int
var eventData models.Event
var marsh marshaller.Marshaller = marshaller.NewMarshaller()

func ProcessData() {
	value, isPresent := aggData[eventData.ActionType]
	if !isPresent {
		value = 0
	}

	aggData[eventData.ActionType] = value + eventData.Count
}

func PrettyPrint() {
	for key, value := range aggData {
		fmt.Println("There are ", value, "for action type: ", key)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	aggData = make(map[string]int)

	fmt.Println("My MOMP Application Consumer")
	fmt.Println("---------------------------------")
	fmt.Println("Write topic name to consume:")
	topic, _ := reader.ReadString('\n')
	topic = strings.Replace(topic, "\n", "", -1)
	fmt.Println("Write ur own identifier to consume:")
	identifier, _ := reader.ReadString('\n')
	identifier = strings.Replace(topic, "\n", "", -1)

	conn, _ := net.Dial("tcp", "localhost:5556")
	subscriber, _ := consumer.NewSubscriber(conn)
	subscriber.Subscribe(topic, identifier)

	for {
		subscriber.Receive(&eventData)
		fmt.Println("---------------------------------")
		fmt.Println("Data consumed from topic: ", topic)
		ProcessData()
		fmt.Println("Aggregated data:")
		PrettyPrint()
	}
}
