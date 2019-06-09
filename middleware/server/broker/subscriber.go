package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"net"
)

type Subscriber struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
}

func (s *Subscriber) Send(m *models.Response) error {
	err := s.jsonEncoder.Encode(m.Body)
	return err
}
