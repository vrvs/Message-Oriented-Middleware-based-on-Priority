package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	queueManager "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
	"encoding/json"
	"net"
	"sync"
	"time"
)

type Broker interface {
	Subscribe(subscriberConnection net.Conn, topicName string, identifier string) error
	Publish(topicName string, messagePrioriy int, data []byte) error
	BroadcastTopic(topic *Topic) error
	BroadcastConn()
	BroadcastNotConn()
	CreateTopic(topicName string, maxPriority int, retry bool) error
}

type broker struct {
	topics       *Topics
	queueManager queueManager.QueueManager
}

func NewBroker() Broker {
	return &broker{
		topics:       NewTopics(),
		queueManager: queueManager.NewQueueManager(),
	}
}

func (b *broker) Subscribe(subscriberConnection net.Conn, topicName string, identifier string) error {
	topic, err := b.topics.GetTopic(topicName)
	if err != nil {
		b.CreateTopic(topicName, 10, false)
		topic, _ = b.topics.GetTopic(topicName)
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

func (b *broker) Publish(topicName string, messagePrioriy int, data []byte) error {
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
func (b *broker) BroadcastTopic(topic *Topic) error {
	topic, err := b.topics.GetTopic(topic.Name)
	if err != nil {
		return err
	}

	if len(topic.Subscribers) < 1 {
		return nil
	}

	m, err := b.queueManager.Pop(topic.Name)
	if err != nil {
		return err
	}

	for _, s := range topic.Subscribers {
		err := s.Send(&models.Response{
			Body: m.(*priority.Item).Value,
		})
		if err != nil {
			top, _ := b.topics.GetTopic(s.identifier)
			if !b.topics.TopicExists(s.identifier) {
				b.CreateTopic(s.identifier, topic.MaxPriority, true)
				top, _ = b.topics.GetTopic(s.identifier)
				top.AddSubscriber(s)
			} else if top.IsConn {
				top.IsConn = false
			}
			b.queueManager.Push(s.identifier, m)
		} else {
			if !topic.IsConn {
				topic.IsConn = true
			}
		}
	}
	return nil
}

func (b *broker) BroadcastConn() {
	for {
		topics := b.topics.GetTopics()
		for i := 0; i < len(topics); i++ {
			if topics[i].IsConn {
				go b.BroadcastTopic(topics[i])
			}
		}
	}
}

func (b *broker) BroadcastNotConn() {
	for {
		time.Sleep(35000000000)
		topics := b.topics.GetTopics()
		for i := 0; i < len(topics); i++ {
			if !topics[i].IsConn {
				go b.BroadcastTopic(topics[i])
			}
		}
	}
}

func (b *broker) CreateTopic(topicName string, maxPriority int, retry bool) error {

	topic := &Topic{
		Name:        topicName,
		Subscribers: []*Subscriber{},
		MaxPriority: maxPriority,
		Lock:        sync.RWMutex{},
		Retry:       retry,
		IsConn:      !retry,
	}

	err := b.topics.AddTopic(topic)
	if err != nil {
		return err
	}

	b.queueManager.CreateQueue(topicName)
	return nil
}
