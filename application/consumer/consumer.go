package main

import (
	"net"
	"bufio"
  	"fmt"
  	"os"
	"strings"
	"Producer-Side-Application/jars/com/momp/consumer"
	"Producer-Side-Application/jars/com/momp/lib/marshaller"
	"Producer-Side-Application/app/models"
	"encoding/json"
)

var aggData map[string]int
var marsh marshaller.Marshaller = marshaller.NewMarshaller()

func EventFromJson(value []byte) models.Event {
	var data models.Event

	json.Unmarshal(value, &data)

	return data
}

func ProcessData(data []byte) {
	eventData := EventFromJson(data)
	
	value, isPresent := aggData[eventData.ActionType]
	if (!isPresent) {
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

	fmt.Println("My MOMP Application Producer")
  	fmt.Println("---------------------------------")
	fmt.Println("Write topic name to produce:")
	topic, _ := reader.ReadString('\n')
	topic = strings.Replace(topic, "\n", "", -1)

	conn, _ := net.Dial("tcp", "localhost:5556")
	subscriber, _ := consumer.NewSubscriber(conn)
	subscriber.Subscribe(topic)

	for {
		data, _ := subscriber.Receive()
		fmt.Println("---------------------------------")
		fmt.Println("Data consumed from topic: ", topic)
		ProcessData(data)
		fmt.Println("Aggregated data:")
		PrettyPrint()
	}
}