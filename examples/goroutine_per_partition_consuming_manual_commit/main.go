package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type pconsumer struct {
	quit chan struct{}
	recs chan []*kgo.Record
}

var (
	topic = flag.String("t", "", "topic to consume")
	group = flag.String("g", "", "group to consume in")
)

func (pc *pconsumer) consume(topic string, partition int32, cl *kgo.Client) {
	zap.L().Info("Starting consume", zap.String("topic", topic))
	defer zap.L().Info("Killing consume consume", zap.String("topic", topic))
	for {
		select {
		case <-pc.quit:
			zap.L().Info("Consume quitting")
			return
		case recs := <-pc.recs:
			fmt.Print(".")

			err := cl.CommitRecords(context.Background(), recs...)

			if err != nil {
				zap.L().Error("Error when committing offsets to kafka",
					zap.Error(err),
					zap.Int64("Commited offset", recs[len(recs)-1].Offset+1),
				)
			}
			time.Sleep(100 * time.Millisecond)
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
			zap.L().Info("Assigning", zap.String("topic", topic))
			pc := pconsumer{
				quit: make(chan struct{}),
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
	fmt.Println("REVOKED!!!!!!!!")
	for topic, partitions := range lost {
		ptopics := s.consumers[topic]
		for _, partition := range partitions {
			pc := ptopics[partition]
			delete(ptopics, partition)
			if len(ptopics) == 0 {
				delete(s.consumers, topic)
			}
			zap.L().Info("Quitting", zap.String("topic", topic))
			close(pc.quit)
		}
	}
}

func main() {
	flag.Parse()
	zap.NewDevelopment()

	s := &splitConsume{
		consumers: make(map[string]map[int32]pconsumer),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers("localhost:9092"),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.lost),
		kgo.OnPartitionsLost(s.lost),
		kgo.DisableAutoCommit(),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	err = cl.Ping(context.Background())
	if err != nil {
		panic(err)
	}

	s.poll(cl)
}

func (s *splitConsume) poll(cl *kgo.Client) {
	for {
		fetches := cl.PollRecords(context.Background(), 10)
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
					zap.L().Info("Quit case in poll")
				}
			})
		})
	}
}
