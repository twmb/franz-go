package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type pconsumer struct {
	quit chan struct{}
	done chan struct{}
	recs chan []*kgo.Record
}

var (
	brokers = flag.String("b", "", "comma delimited brokers to consume from")
	topic   = flag.String("t", "", "topic to consume")
	group   = flag.String("g", "", "group to consume in")
)

func (pc *pconsumer) consume(topic string, partition int32, cl *kgo.Client) {
	fmt.Printf("Starting consume for  t %s p %d\n", topic, partition)
	for {
		select {
		case <-pc.quit:
			pc.done <- struct{}{}
			fmt.Printf("Closing consume for t %s p %d\n", topic, partition)
			return
		case recs := <-pc.recs:
			// Mimick work to happen before committing records
			time.Sleep(time.Duration(rand.Intn(150)+100) * time.Millisecond)
			fmt.Printf("Some sort of work done, about to commit t %s p %d\n", topic, partition)
			err := cl.CommitRecords(context.Background(), recs...)
			if err != nil {
				fmt.Printf("Error when committing offsets to kafka err: %v t: %s p: %d offset %d\n", err, topic, partition, recs[len(recs)-1].Offset+1)
			}
		}
	}
}

type splitConsume struct {
	mu        sync.Mutex // gaurds assigning / losing vs. polling
	consumers map[string]map[int32]pconsumer
}

func (s *splitConsume) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, partitions := range assigned {
		if s.consumers[topic] == nil {
			s.consumers[topic] = make(map[int32]pconsumer)
		}
		for _, partition := range partitions {
			pc := pconsumer{
				quit: make(chan struct{}),
				done: make(chan struct{}),
				recs: make(chan []*kgo.Record),
			}
			s.consumers[topic][partition] = pc
			go pc.consume(topic, partition, cl)
		}
	}
}

func (s *splitConsume) lost(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, partitions := range lost {
		ptopics := s.consumers[topic]
		for _, partition := range partitions {
			pc := ptopics[partition]
			delete(ptopics, partition)
			if len(ptopics) == 0 {
				delete(s.consumers, topic)
			}
			close(pc.quit)
			fmt.Printf("Waiting for work to finish t %s p %d\n", topic, partition)
			<-pc.done
		}
	}
}

func main() {
	rand.Seed(time.Now().Unix())
	flag.Parse()

	if len(*group) == 0 {
		fmt.Println("missing required group")
		return
	}
	if len(*topic) == 0 {
		fmt.Println("missing required topic")
		return
	}

	s := &splitConsume{
		consumers: make(map[string]map[int32]pconsumer),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.lost),
		kgo.OnPartitionsLost(s.lost),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	// Check connectivity to cluster
	err = cl.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	s.poll(cl)
}

func (s *splitConsume) poll(cl *kgo.Client) {
	for {
		fetches := cl.PollRecords(context.Background(), 10000)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			panic(err)
		})
		fetches.EachTopic(func(t kgo.FetchTopic) {
			s.mu.Lock()
			tconsumers := s.consumers[t.Topic]
			s.mu.Unlock()
			if tconsumers == nil {
				return
			}
			t.EachPartition(func(p kgo.FetchPartition) {
				pc, ok := tconsumers[p.Partition]
				if !ok {
					return
				}
				select {
				case pc.recs <- p.Records:
				case <-pc.quit:
				}
			})
		})
		s.mu.Lock()
		cl.AllowRebalance()
		s.mu.Unlock()
	}
}
