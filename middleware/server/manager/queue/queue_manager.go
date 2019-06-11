package queue

import (
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
	"container/heap"
	"errors"
	"sync"
)

type safePriorityQueue struct {
	priorityQueue priority.PriorityQueue
	lock          *sync.RWMutex
}
type queueManager struct {
	queues map[string]*safePriorityQueue
	lock   *sync.RWMutex
}

type QueueManager interface {
	Pop(topic string) (interface{}, error)
	Push(topic string, item interface{}) error
	Len(topic string) (int, error)
	CreateQueue(topic string) error
}

func NewQueueManager() QueueManager {
	return &queueManager{
		queues: make(map[string]*safePriorityQueue),
		lock:   &sync.RWMutex{},
	}
}

func (qm *queueManager) Pop(topic string) (interface{}, error) {
	qm.lock.RLock()
	defer qm.lock.RUnlock()
	queue, ok := qm.queues[topic]
	if !ok {
		return nil, errors.New("Queue not exists")
	}
	queue.lock.Lock()
	defer queue.lock.Unlock()
	if queue.priorityQueue.Len() == 0 {
		return nil, errors.New("empty queue")
	}
	val := heap.Pop(&queue.priorityQueue)
	return val, nil
}

func (qm *queueManager) Push(topic string, item interface{}) error {
	qm.lock.RLock()
	defer qm.lock.RUnlock()
	queue, ok := qm.queues[topic]
	if !ok {
		return errors.New("Queue not exists")
	}
	queue = qm.queues[topic]
	queue.lock.Lock()
	heap.Push(&queue.priorityQueue, item)
	queue.lock.Unlock()
	return nil
}

func (qm *queueManager) Len(topic string) (int, error) {
	qm.lock.RLock()
	defer qm.lock.RUnlock()
	queue, ok := qm.queues[topic]
	if !ok {
		return 0, errors.New("Queue not exists")
	}
	queue.lock.RLock()
	len := queue.priorityQueue.Len()
	queue.lock.RUnlock()
	return len, nil
}

func (qm *queueManager) CreateQueue(topic string) error {
	qm.lock.RLock()
	_, ok := qm.queues[topic]
	qm.lock.RUnlock()
	if ok {
		return errors.New("Queue already exists")
	}
	qm.lock.Lock()
	qm.queues[topic] = &safePriorityQueue{
		priorityQueue: make(priority.PriorityQueue, 0),
		lock:          &sync.RWMutex{},
	}
	qm.lock.Unlock()
	return nil
}
