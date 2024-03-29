package models

type Message struct {
	Head            string
	TopicName       string
	MaxPriority     int
	MessagePriority int
	Body            []byte
	Identifier      string
}

type Response struct {
	Body  []byte
	Error string
}
