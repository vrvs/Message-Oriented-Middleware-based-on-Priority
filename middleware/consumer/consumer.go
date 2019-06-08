package consumer

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"bufio"
	"errors"
	"fmt"
	"net"
)

var marsh = marshaller.NewMarshaller()

type Subscriber interface {
	Subscribe(topicName string, conn net.Conn) error
	Receive() []byte
}

type subscriber struct {
	conn net.Conn
}

func NewSubscriber(conn net.Conn) (Subscriber, error) {
	if conn == nil {
		return nil, errors.New("error: empty conn")
	}

	return &subscriber{
		conn: conn,
	}, nil
}

func (s *subscriber) Subscribe(topicName string, conn net.Conn) error {
	if conn == nil {
		return errors.New("error: empty conn")
	}

	msg := models.Message{
		Head:      "Subscribe",
		TopicName: topicName,
		Conn:      conn,
	}

	s.send(msg)

	return nil
}

func (s *subscriber) Receive() []byte {
	message, _ := bufio.NewReader(s.conn).ReadString('\n')

	msgUnmarshalled := marsh.Unmarshall([]byte(message))

	ans := msgUnmarshalled.(models.Subscribing)

	return ans.Body
}

func (s *subscriber) send(msg models.Message) {
	msgMarshalled := marsh.Marshall(msg)
	fmt.Fprintf(s.conn, string(msgMarshalled)+"\n")
}
