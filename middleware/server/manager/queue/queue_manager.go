package queue

import (
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
	"container/heap"
	"errors"
	"fmt"
	"sync"
)

type safePriorityQueue struct {
	priorityQueue priority.PriorityQueue
	lock          *sync.RWMutex
}
type QueueManager struct {
	queues map[string]*safePriorityQueue
	lock   *sync.RWMutex
}

func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*safePriorityQueue),
		lock:   &sync.RWMutex{},
	}
}

func (qm *QueueManager) Pop(topic string) (interface{}, error) {
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

func (qm *QueueManager) Push(topic string, item interface{}) error {
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

func (qm *QueueManager) Len(topic string) (int, error) {
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

func (qm *QueueManager) CreateQueue(topic string) error {
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

func main() {
	manager := &QueueManager{
		queues: make(map[string]*safePriorityQueue),
		lock:   &sync.RWMutex{},
	}
	item := &priority.Item{
		Value:    nil,
		Priority: 7,
	}
	item2 := &priority.Item{
		Value:    nil,
		Priority: 9,
	}
	item3 := &priority.Item{
		Value:    nil,
		Priority: 4,
	}
	manager.Push("hello", item)
	manager.Push("hello", item2)
	manager.Push("hello", item3)
	manager.Push("hi", item)
	manager.Push("hi", item2)
	manager.Push("hi", item3)
	v, _ := manager.Pop("hello")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
	v, _ = manager.Pop("hello")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
	v, _ = manager.Pop("hello")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
	v, _ = manager.Pop("hi")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
	v, _ = manager.Pop("hi")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
	v, _ = manager.Pop("hi")
	fmt.Printf("%d\n", v.(*priority.Item).Priority)
}
