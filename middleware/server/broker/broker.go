package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	queueManager "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"

	"net"
	"sync"
)

type Broker struct {
	topics       *Topics
	slock        sync.RWMutex
	tlock        sync.RWMutex
	queueManager *queueManager.QueueManager
}

func NewBroker() *Broker {
	return &Broker{
		topics:       &Topics{},
		slock:        sync.RWMutex{},
		tlock:        sync.RWMutex{},
		queueManager: &queueManager.QueueManager{},
	}
}

func (b *Broker) Subscribe(subscriberConnection net.Conn, topicID string) error {
	b.tlock.Lock()
	defer b.tlock.Unlock()

	topic, err := b.topics.GetTopic(topicID)
	if err != nil {
		return err
	}

	sub := &Subscriber{
		conn:      subscriberConnection,
		publisher: false,
		lock:      &sync.RWMutex{},
	}

	topic.Subscribers = append(topic.Subscribers, *sub)

	return nil
}

func (b *Broker) Publish(topicName string, messagePrioriy int, data []byte) error {
	err := b.queueManager.Push(topicName, priority.Item{
		Value:    data,
		Priority: messagePrioriy,
	})
	if err != nil {
		return err
	}

	return nil
}

/*
func (b *Broker) Unsubscribe(subscriberID, topicID string) error {
}
*/
func (b *Broker) Broadcast(topicName string) error {
	topic, err := b.topics.GetTopic(topicName)
	if err != nil {
		return err
	}

	if len(topic.Subscribers) < 1 {
		return nil
	}

	m, err := b.queueManager.Pop(topicName)
	for _, s := range topic.Subscribers {
		err := s.Send(&models.Response{
			Body: m.(*priority.Item).Value,
		})
		if err != nil {
			return err
		}

	}

	return nil
}

func (b *Broker) CreateTopic(topicName string, maxPriority int) (net.Conn, error) {
	ln, err := net.Listen("tcp", "localhost:5555")
	if err != nil {
		return nil, err
	}

	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}

	topic := &Topic{
		Name:        topicName,
		Subscribers: []Subscriber{},
		MaxPriority: maxPriority,
		Conn:        conn,
	}

	err = b.topics.AddTopic(topic)
	if err != nil {
		return nil, err
	}

	b.queueManager.InsertQueue(topicName, topic.Queue)

	return topic.Conn, nil
}
