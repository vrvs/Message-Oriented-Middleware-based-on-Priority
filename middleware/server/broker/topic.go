package broker

import (
	"reflect"
	"sync"
)

type Topic struct {
	Subscribers []*Subscriber
	MaxPriority int
	Name        string
	Lock        sync.RWMutex
	Retry       bool
	Chan        chan (interface{})
}

type Topics struct {
	topics map[string]*Topic
	lock   sync.RWMutex
}

func NewTopics() *Topics {
	return &Topics{
		topics: make(map[string]*Topic),
		lock:   sync.RWMutex{},
	}
}

func (t *Topics) GetTopicsName() []string {
	t.lock.RLock()
	defer t.lock.RUnlock()
	keys := reflect.ValueOf(t.topics).MapKeys()
	strkeys := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		strkeys[i] = keys[i].String()
	}
	return strkeys
}

func (t *Topics) TopicExists(topicName string) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	_, exist := t.topics[topicName]
	return exist
}

func (t *Topics) GetTopic(topicName string) (*Topic, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	topic, exist := t.topics[topicName]
	if !exist {
		return nil, &TopicNotExist{}
	}

	return topic, nil
}

func (t *Topics) AddTopic(topic *Topic) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	_, exist := t.topics[topic.Name]
	if exist {
		return &ExistentTopic{}
	}
	t.topics[topic.Name] = topic
	return nil
}

func (t *Topic) AddSubscriber(sub *Subscriber) error {
	t.Lock.Lock()
	defer t.Lock.Unlock()
	for _, s := range t.Subscribers {
		if s.identifier == sub.identifier {
			s.conn = sub.conn
			s.jsonEncoder = sub.jsonEncoder
			return nil
		}
	}
	t.Subscribers = append(t.Subscribers, sub)
	return nil
}
