package adapter

import (
	"Message-Oriented-Middleware-based-on-Priority/middleware/lib/models"
	"encoding/json"
)

func MessageFromJson(value []byte) models.Message {
	var message models.Message

	json.Unmarshal(value, &message)

	return message
}

func ResponseFromJson(value []byte) models.Response {
	var response models.Response

	json.Unmarshal(value, &response)

	return response
}
