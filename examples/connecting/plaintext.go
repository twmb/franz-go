package connect

import (
	"fmt"

	"github.com/twmb/kafka-go/pkg/kgo"
)

func connectPlaintext() {
	fmt.Println("starting...")

	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Do something with the client
	// ...
}
