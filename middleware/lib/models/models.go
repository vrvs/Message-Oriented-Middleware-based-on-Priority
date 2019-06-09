package models

import "net"

type Message struct {
	Head            string
	TopicName       string
	MaxPriority     int
	MessagePriority int
	Body            []byte
	Conn            net.Conn
}

type Response struct {
	Body  []byte
	Error string
}
