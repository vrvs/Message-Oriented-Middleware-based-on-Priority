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
	if ok {
		queue.lock.Lock()
		defer queue.lock.Unlock()
		if queue.priorityQueue.Len() == 0 {
			return nil, errors.New("empty queue")
		}
		val := heap.Pop(&queue.priorityQueue)
		return val, nil
	}
	return nil, errors.New("empty queue")
}

func (qm *QueueManager) Push(topic string, item interface{}) error {
	qm.lock.RLock()
	queue, ok := qm.queues[topic]
	qm.lock.RUnlock()
	if !ok {
		qm.lock.Lock()
		qm.queues[topic] = &safePriorityQueue{
			priorityQueue: make(priority.PriorityQueue, 0),
			lock:          &sync.RWMutex{},
		}
		qm.lock.Unlock()
	}
	qm.lock.RLock()
	queue = qm.queues[topic]
	queue.lock.Lock()
	heap.Push(&queue.priorityQueue, item)
	queue.lock.Unlock()
	qm.lock.RUnlock()
	return nil
}

func (qm *QueueManager) Len(topic string) int {
	qm.lock.RLock()
	defer qm.lock.RUnlock()
	queue, ok := qm.queues[topic]
	if !ok {
		return 0
	}
	queue.lock.RLock()
	len := queue.priorityQueue.Len()
	queue.lock.RUnlock()
	return len
}

func main() {
	manager := &QueueManager{
		queues: make(map[string]*safePriorityQueue),
		lock:   &sync.RWMutex{},
	}
	item := &priority.Item{
		Value:    "",
		Priority: 7,
	}
	item2 := &priority.Item{
		Value:    "",
		Priority: 9,
	}
	item3 := &priority.Item{
		Value:    "",
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
