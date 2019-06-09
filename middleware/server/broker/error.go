package broker

type (
	TopicNotExist struct{}

	SubscriberNotExist struct{}

	ExistentTopic struct{}
)

func (t *TopicNotExist) Error() string {
	return "this topic doesnt exist"
}

func (t *SubscriberNotExist) Error() string {
	return "this subscriber doesnt exist"
}

func (t *ExistentTopic) Error() string {
	return "this topic name alread exist"
}
