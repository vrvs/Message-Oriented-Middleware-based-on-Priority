package main

import "Message-Oriented-Middleware-based-on-Priority/middleware/server/handler"

func main() {
	go handler.ServerConsumerHandler()
	handler.ServerProducerHandler()
}
