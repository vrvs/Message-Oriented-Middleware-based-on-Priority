package marshaller

import (
	"encoding/json"
	"log"
)

type Marshaller interface {
	Marshall(message interface{}) []byte
	Unmarshall(message []byte) interface{}
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

func (m marshaller) Unmarshall(message []byte) interface{} {
	var messageUnmarshalled interface{}

	err := json.Unmarshal(message, &messageUnmarshalled)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return messageUnmarshalled
}
