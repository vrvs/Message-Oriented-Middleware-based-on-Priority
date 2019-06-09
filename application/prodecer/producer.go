package main

import (
	"Message-Oriented-Middleware-based-on-Priority/application/models"
	"Message-Oriented-Middleware-based-on-Priority/middleware/producer"
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

var maxPriority int

func ProduceData() producer.Publishing {
	dataRand := rand.Intn(5)
	countRand := rand.Intn(9) + 1

	var actionType string
	priority := maxPriority - dataRand

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

	body := models.Event{
		ActionType: actionType,
		Count:      countRand,
	}

	return producer.Publishing{
		Priority: priority,
		Body:     body,
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("My MOMP Application Producer")
	fmt.Println("---------------------")
	fmt.Println("Write topic name to produce:")
	topic, _ := reader.ReadString('\n')
	topic = strings.Replace(topic, "\n", "", -1)

	conn, _ := net.Dial("tcp", "127.0.0.1:5555")

	publisher, err := producer.NewPublisher(conn)
	if err != nil {
		fmt.Println(err)
	}

	maxPriority = rand.Intn(5) + 5
	publisher.TopicDeclare(topic, maxPriority)

	for {
		data := ProduceData()
		publisher.Publish(topic, data)

		fmt.Println("Published data on topic: ", topic, " with priority: ", data.Priority)

		time.Sleep(time.Duration(rand.Intn(200)+200) * time.Millisecond)
	}
}
