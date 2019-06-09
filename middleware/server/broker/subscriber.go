package broker

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"net"
	"sync"
)

type Subscriber struct {
	conn      net.Conn
	publisher bool
	lock      *sync.RWMutex
}

func (s *Subscriber) Send(m *models.Response) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	_, err := s.conn.Write(m.Body)

	return err
}
