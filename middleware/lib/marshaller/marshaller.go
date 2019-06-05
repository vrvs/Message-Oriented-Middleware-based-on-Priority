package marshaller

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
	"log"
)

type Marshaller interface {
	Marshall(message interface{}) []byte
	Unmarshall(message []byte) *models.Message
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

func (m marshaller) Unmarshall(message []byte) *models.Message {
	var messageUnmarshalled models.Message

	err := json.Unmarshal(message, &messageUnmarshalled)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	return &messageUnmarshalled
}
