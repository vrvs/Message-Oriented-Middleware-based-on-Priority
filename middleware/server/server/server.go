package main

import "Message-Oriented-Middleware-based-on-Priority/middleware/server/handler"

func main() {
	consumer, _ := handler.NewConsumerHandler()
	go consumer.Listen()
}
