package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

func Consume(topics []string, servers string, msgChan chan *kafka.Message) {
	kafkaConsume, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": servers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	kafkaConsume.SubscribeTopics(topics, nil)

	for {
		msg, err := kafkaConsume.ReadMessage(-1)
		if err == nil {
			msgChan <- msg
		}
	}
}
