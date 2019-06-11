package marshaller

import (
	"encoding/json"
	"log"
)

type Marshaller interface {
	Marshall(message interface{}) []byte
	Unmarshall(message []byte, response interface{}) error
}

type marshaller struct{}

func NewMarshaller() Marshaller {
	return marshaller{}
}

func (m marshaller) Marshall(message interface{}) []byte {
	msgMarshalled, err := json.Marshal(message)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return msgMarshalled
}

func (m marshaller) Unmarshall(message []byte, response interface{}) error {

	err := json.Unmarshal(message, &response)

	if err != nil {
		return nil
	}

	return nil
}
