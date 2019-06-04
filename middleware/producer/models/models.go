package models

type Message struct {
	Head            string
	QueueName       string
	MaxPriority     int64
	MessagePriority int64
	Body            []byte
}
