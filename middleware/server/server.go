package main

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/broker"
	"Message-Oriented-Middleware-based-on-Priority/middleware/server/handler"
)

func main() {
	broker := broker.NewBroker()
	go handler.ServerConsumerHandler(broker)
	handler.ServerProducerHandler(broker)
}
