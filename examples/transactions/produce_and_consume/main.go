package main

import "context"

func main() {
	kafkaBrokers := "localhost:9092"
	topic := "test"

	ctx := context.Background()
	go startConsuming(ctx, kafkaBrokers, topic)
	startProducing(ctx, kafkaBrokers, topic)
}
