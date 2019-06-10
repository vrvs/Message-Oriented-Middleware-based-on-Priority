package main

import (
	"encoding/json"
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"strings"
	"bufio"
	"os"
	"Message-Oriented-Middleware-based-on-Priority/rabbit-app/models"
)

var aggData map[string]int

func ProcessData(eventData models.Event) {	
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

func get(msgs *<-chan amqp.Delivery) models.Event {
	msg := <-*msgs
	req := models.Event{}
	err := json.Unmarshal(msg.Body, &req)
	if err != nil {
		log.Fatalf("Failed to unmarshal request")
	}

	return req
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel")
	}
	defer ch.Close()

	aggData = make(map[string]int)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("My MOMP Application Producer")
  	fmt.Println("---------------------")
	fmt.Println("Write topic name to produce:")
	topic, _ := reader.ReadString('\n')
	topic = strings.Replace(topic, "\n", "", -1)

	consumerChannel, err := ch.Consume(topic, "", true, false, false, false, nil)

	for {
		message := get(&consumerChannel)
		fmt.Println("---------------------------------")
		fmt.Println("Data consumed from topic: ", topic)
		ProcessData(message)
		fmt.Println("Aggregated data:")
		PrettyPrint()
	}
	
}