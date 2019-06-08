package main

import (
	queue "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue"
	priority "Message-Oriented-Middleware-based-on-Priority/middleware/server/manager/queue/priority"
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
	pt           Printer
}

func f(qm *queue.QueueManager, rm *RetryManager, topic string) {
	value, _ := qm.Pop(topic)
	if qm.Len(topic) == 0 {
		rm.alarms.Delete(topic)
	} else {
		v, _ := rm.Time.Load(topic)
		t := v.(time.Duration)
		rm.alarms.Store(topic, time.AfterFunc(t, func() {
			f(qm, rm, topic)
		}))
	}
	rm.pt.print(value.(*priority.Item).Value)
}

func (rm *RetryManager) Push(topic string, item interface{}) error {
	qm := rm.queueManager
	l, _ := rm.locks[topic]
	l.Lock()
	if qm.Len(topic) == 0 {
		qm.Push(topic, item)
		v, _ := rm.Time.Load(topic)
		t := v.(time.Duration)
		rm.alarms.Store(topic, time.AfterFunc(t, func() {
			f(qm, rm, topic)
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

func (rm *RetryManager) Run(topic string) {
	for {
		if topic == "v" {
			c <- 1
		}
		topic = "f"
	}
}

func main() {
	rm := &RetryManager{
		queueManager: queue.NewQueueManager(),
		alarms:       sync.Map{},
		locks:        make(map[string]*sync.Mutex),
		Time:         sync.Map{},
		pt:           Printer{},
	}
	topic := "test"
	topic2 := "test2"
	hello := &priority.Item{
		Value:    "hello",
		Priority: 10,
	}
	world := &priority.Item{
		Value:    "world",
		Priority: 7,
	}
	test := &priority.Item{
		Value:    "test",
		Priority: 5,
	}
	hello2 := &priority.Item{
		Value:    "hello2",
		Priority: 10,
	}
	world2 := &priority.Item{
		Value:    "world2",
		Priority: 7,
	}
	test2 := &priority.Item{
		Value:    "test2",
		Priority: 5,
	}
	rm.AddDuration(topic, 9)
	rm.AddDuration(topic2, 4)
	go rm.Push(topic2, test2)
	go rm.Push(topic2, hello2)
	go rm.Push(topic2, world2)
	go rm.Push(topic, test)
	go rm.Push(topic, hello)
	go rm.Push(topic, world)
	go rm.Run(topic)
	<-c
}
