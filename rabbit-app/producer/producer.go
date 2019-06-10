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
	"math/rand"
	"time"
)

func ProduceData() models.Event {
	dataRand := rand.Intn(5)
	countRand := rand.Intn(9) + 1

	var actionType string

	switch dataRand {
	case 0:
		actionType = "ad-click"
	case 1:
		actionType = "link-click"
	case 2:
		actionType = "ad-view"
	case 3:
		actionType = "prod-view"
	case 4:
		actionType = "misclick"
	default:
	}

	return models.Event {
		ActionType: actionType,
		Count: countRand, 
	}
}

func send(responseQueue amqp.Queue, msg models.Event, ch *amqp.Channel) {

	response, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Failed to marshal the response")
	}

	err = ch.Publish("", responseQueue.Name, false, false,
		amqp.Publishing{
			ContentType: "json",
			Body:        []byte(response),
		},
	)
	if err != nil {
		log.Fatalf("Failed to publish the response")
	}
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

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("My MOMP Application Producer")
  	fmt.Println("---------------------")
	fmt.Println("Write topic name to produce:")
	topic, _ := reader.ReadString('\n')
	topic = strings.Replace(topic, "\n", "", -1)

	topicQueue, err := ch.QueueDeclare(topic, false, false, false, false, nil)

	for {
		data := ProduceData()
		send(topicQueue, data, ch)

		fmt.Println("Published data on topic: ", topic)

		time.Sleep(time.Duration(rand.Intn(200) + 200) * time.Millisecond)
	}
}