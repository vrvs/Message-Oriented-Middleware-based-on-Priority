package models

import "net"

type Message struct {
	Head            string
	TopicName       string
	MaxPriority     int64
	MessagePriority int64
	Body            []byte
	Conn            net.Conn
}

type Response struct {
	Body  []byte
	Error string
}
