package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	queueManager "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
	"encoding/json"
	"net"
	"time"
)

type Broker struct {
	topics       *Topics
	queueManager *queueManager.QueueManager
}

func NewBroker() *Broker {
	return &Broker{
		topics:       NewTopics(),
		queueManager: queueManager.NewQueueManager(),
	}
}

func (b *Broker) Subscribe(subscriberConnection net.Conn, topicID string) error {
	topic, err := b.topics.GetTopic(topicID)
	if err != nil {
		return err
	}

	sub := &Subscriber{
		conn:        subscriberConnection,
		jsonEncoder: json.NewEncoder(subscriberConnection),
		marshaller:  marshaller.NewMarshaller(),
	}

	err = topic.AddSubscriber(sub)
	return err
}

func (b *Broker) Publish(topicName string, messagePrioriy int, data []byte) error {
	err := b.queueManager.Push(topicName, &priority.Item{
		Value:    data,
		Priority: messagePrioriy,
	})
	return err
}

/*
func (b *Broker) Unsubscribe(subscriberID, topicID string) error {
}
*/
func (b *Broker) BroadcastTopic(topicName string) error {
	topic, err := b.topics.GetTopic(topicName)
	if err != nil {
		return err
	}

	if len(topic.Subscribers) < 1 {
		return nil
	}

	m, err := b.queueManager.Pop(topicName)
	if err != nil {
		return err
	}
	for _, s := range topic.Subscribers {
		err := s.Send(&models.Response{
			Body: m.(*priority.Item).Value,
		})
		if err != nil {
			//return err
		}
	}
	return nil
}

func (b *Broker) Broadcast() {
	for {
		time.Sleep(3000000000)
		topics := b.topics.GetTopicsName()
		for i := 0; i < len(topics); i++ {
			b.BroadcastTopic(topics[i])
		}
	}
}

func (b *Broker) CreateTopic(topicName string, maxPriority int) error {

	topic := &Topic{
		Name:        topicName,
		Subscribers: []Subscriber{},
		MaxPriority: maxPriority,
	}

	err := b.topics.AddTopic(topic)
	if err != nil {
		return err
	}

	b.queueManager.CreateQueue(topicName)

	return nil
}
