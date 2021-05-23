package main

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
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
