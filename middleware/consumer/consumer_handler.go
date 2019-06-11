package consumer

import (
	"encoding/json"
	"net"
)

type consumerHanlder struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
	jsonDecoder *json.Decoder
}

func newConsumerHanlder(conn net.Conn) *consumerHanlder {
	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return &consumerHanlder{
		conn:        conn,
		jsonEncoder: jsonEncoder,
		jsonDecoder: jsonDecoder,
	}
}

func (c *consumerHanlder) send(msg []byte) {
	c.jsonEncoder.Encode(msg)
}

func (c *consumerHanlder) receive() ([]byte, error) {
	var response []byte
	err := c.jsonDecoder.Decode(&response)

	return response, err
}
