package models

type Message struct {
	Head            string
	TopicName       string
	MaxPriority     int64
	MessagePriority int64
	Body            []byte
}
