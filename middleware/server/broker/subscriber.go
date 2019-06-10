package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/marshaller"
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"net"
)

type Subscriber struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
	marshaller  marshaller.Marshaller
	identifier  string
}

func (s *Subscriber) Send(m *models.Response) error {
	msg := s.marshaller.Marshall(m)
	err := s.jsonEncoder.Encode(msg)
	return err
}
