package producer

import (
	"encoding/json"
	"net"
)

type producerHanlder struct {
	conn        net.Conn
	jsonEncoder *json.Encoder
	jsonDecoder *json.Decoder
}

func newProducerHanlder(conn net.Conn) *producerHanlder {
	jsonEncoder := json.NewEncoder(conn)
	jsonDecoder := json.NewDecoder(conn)

	return &producerHanlder{
		conn:        conn,
		jsonEncoder: jsonEncoder,
		jsonDecoder: jsonDecoder,
	}
}

func (p *producerHanlder) send(msg []byte) {
	p.jsonEncoder.Encode(msg)
}

func (p *producerHanlder) receive() ([]byte, error) {
	var response []byte
	err := p.jsonDecoder.Decode(&response)

	return response, err
}
