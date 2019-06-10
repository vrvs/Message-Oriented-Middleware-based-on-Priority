package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	queueManager "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
	retryManager "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/retry"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

type Broker struct {
	topics       *Topics
	queueManager *queueManager.QueueManager
	retryManager *retryManager.RetryManager
}

func NewBroker() *Broker {
	return &Broker{
		topics:       NewTopics(),
		queueManager: queueManager.NewQueueManager(),
		retryManager: retryManager.NewRetryManager(),
	}
}

func (b *Broker) Subscribe(subscriberConnection net.Conn, topicID string, identifier string) error {
	topic, err := b.topics.GetTopic(topicID)
	if err != nil {
		return err
	}

	sub := &Subscriber{
		conn:        subscriberConnection,
		jsonEncoder: json.NewEncoder(subscriberConnection),
		marshaller:  marshaller.NewMarshaller(),
		identifier:  identifier,
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

	var m interface{}
	if topic.Retry {
		m = <-topic.Chan
	} else {
		m, err = b.queueManager.Pop(topicName)
	}
	if err != nil {
		return err
	}

	for _, s := range topic.Subscribers {
		err := s.Send(&models.Response{
			Body: m.(*priority.Item).Value,
		})
		fmt.Println(err)
		fmt.Println(s.conn)
		fmt.Println(topic.Name)
		if err != nil {
			top, _ := b.topics.GetTopic(s.identifier)
			if !b.topics.TopicExists(s.identifier) {
				b.CreateTopic(s.identifier, topic.MaxPriority, true)
				top, _ = b.topics.GetTopic(s.identifier)
				top.AddSubscriber(s)
			}
			b.retryManager.Push(s.identifier, m, top.Chan)
		}
	}
	return nil
}

func (b *Broker) Broadcast() {
	for {
		time.Sleep(3000000000)
		topics := b.topics.GetTopicsName()
		for i := 0; i < len(topics); i++ {
			go b.BroadcastTopic(topics[i])
		}
	}
}

func (b *Broker) CreateTopic(topicName string, maxPriority int, retry bool) error {

	topic := &Topic{
		Name:        topicName,
		Subscribers: []*Subscriber{},
		MaxPriority: maxPriority,
		Lock:        sync.RWMutex{},
		Retry:       retry,
		Chan:        make(chan interface{}),
	}

	err := b.topics.AddTopic(topic)
	if err != nil {
		return err
	}

	if retry {
		b.retryManager.AddDuration(topicName, 30)
		b.retryManager.CreateQueue(topicName)
	} else {
		b.queueManager.CreateQueue(topicName)
	}
	return nil
}
