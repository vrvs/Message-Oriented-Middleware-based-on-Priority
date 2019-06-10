package retry

import (
	queue "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Printer struct{}

var c = make(chan int)

func (pt *Printer) print(item interface{}) {
	fmt.Println(item)
}

type RetryManager struct {
	queueManager *queue.QueueManager
	alarms       sync.Map // [string]*time.Timer
	locks        map[string]*sync.Mutex
	Time         sync.Map //map[string]time.Duration
}

func NewRetryManager() *RetryManager {
	return &RetryManager{
		queueManager: queue.NewQueueManager(),
		alarms:       sync.Map{},
		locks:        make(map[string]*sync.Mutex),
		Time:         sync.Map{},
	}
}
func f(qm *queue.QueueManager, rm *RetryManager, topic string, c chan<- interface{}) {
	value, _ := qm.Pop(topic)
	len, _ := qm.Len(topic)
	if len == 0 {
		rm.alarms.Delete(topic)
	} else {
		v, _ := rm.Time.Load(topic)
		t := v.(time.Duration)
		rm.alarms.Store(topic, time.AfterFunc(t, func() {
			f(qm, rm, topic, c)
		}))
	}
	c <- value
}

func (rm *RetryManager) Push(topic string, item interface{}, c chan<- interface{}) error {
	qm := rm.queueManager
	l, _ := rm.locks[topic]
	l.Lock()
	len, _ := qm.Len(topic)
	if len == 0 {
		qm.Push(topic, item)
		v, _ := rm.Time.Load(topic)
		t := v.(time.Duration)
		fmt.Println(t)
		rm.alarms.Store(topic, time.AfterFunc(t, func() {
			f(qm, rm, topic, c)
		}))
	} else {
		qm.Push(topic, item)
	}
	l.Unlock()
	return nil
}

func (rm *RetryManager) AddDuration(topic string, seconds int) {
	secondsStr := strconv.Itoa(seconds) + "s"
	duration, _ := time.ParseDuration(secondsStr)
	rm.Time.Store(topic, duration)
	rm.locks[topic] = &sync.Mutex{}
}

func (rm *RetryManager) CreateQueue(topic string) error {
	qm := rm.queueManager
	l, _ := rm.locks[topic]
	l.Lock()
	l.Unlock()
	return qm.CreateQueue(topic)
}
