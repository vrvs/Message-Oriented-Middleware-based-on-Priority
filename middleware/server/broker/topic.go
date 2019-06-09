package broker

import (
	queue "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	"net"
)

type Topic struct {
	Subscribers []Subscriber
	MaxPriority int
	Queue       *queue.SafePriorityQueue
	Name        string
	Conn        net.Conn
}

type Topics struct {
	topics map[string]*Topic
}

func (t *Topics) GetTopic(topicName string) (*Topic, error) {
	topic, exist := t.topics[topicName]
	if !exist {
		return nil, &TopicNotExist{}
	}

	return topic, nil
}

func (t *Topics) AddTopic(topic *Topic) error {
	_, exist := t.topics[topic.Name]
	if exist {
		return &ExistentTopic{}
	}

	return nil
}
