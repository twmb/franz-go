package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers = flag.String("b", "", "comma delimited brokers to consume from")
	topic   = flag.String("t", "", "topic to consume")
	group   = flag.String("g", "", "group to consume in")
)

var (
	globalRecs  int64
	globalBytes int64
)

type pconsumer struct {
	quit chan struct{}
	recs chan []*kgo.Record
}

func (pc *pconsumer) consume(topic string, partition int32) {
	fmt.Printf("starting, t %s p %d\n", topic, partition)
	defer fmt.Printf("killing, t %s p %d\n", topic, partition)
	var (
		nrecs  int
		nbytes int
		ticker = time.NewTicker(time.Second)
	)
	defer ticker.Stop()
	for {
		select {
		case <-pc.quit:
			return
		case recs := <-pc.recs:
			nrecs += len(recs)
			atomic.AddInt64(&globalRecs, int64(len(recs)))
			for _, rec := range recs {
				nbytes += len(rec.Value)
				atomic.AddInt64(&globalBytes, int64(len(rec.Value)))
			}

		case t := <-ticker.C:
			fmt.Printf("[%s] t %s p %d consumed %0.2f MiB/s, %0.2fk records/s\n",
				t.Format("15:04:05.999"),
				topic,
				partition,
				float64(nbytes)/(1024*1024),
				float64(nrecs)/1000,
			)
			nrecs, nbytes = 0, 0
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
				recs: make(chan []*kgo.Record, 10),
			}
			s.consumers[topic][partition] = pc
			go pc.consume(topic, partition)
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
		}
	}
}

func main() {
	flag.Parse()

	go func() {
		for t := range time.Tick(time.Second) {
			fmt.Printf("[%s] globally consumed %0.2f MiB/s, %0.2fk records/s\n",
				t.Format("15:04:05.999"),
				float64(atomic.SwapInt64(&globalBytes, 0))/(1024*1024),
				float64(atomic.SwapInt64(&globalRecs, 0))/1000,
			)
		}
	}()

	s := &splitConsume{
		consumers: make(map[string]map[int32]pconsumer),
	}

	if len(*group) == 0 {
		fmt.Println("missing required group")
		return
	}
	if len(*topic) == 0 {
		fmt.Println("missing required topic")
		return
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.lost),
		kgo.OnPartitionsLost(s.lost),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)
	go func() {
		<-sigs
		fmt.Println("received interrupt signal; closing client")
		cl.Close()
		<-sigs
		fmt.Println("received second interrupt; exiting")
		os.Exit(1)
	}()

	s.poll(cl)
}

func (s *splitConsume) poll(cl *kgo.Client) {
	for {
		fetches := cl.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			// Note: you can delete this block, which will result
			// in these errors being sent to the partition
			// consumers, and then you can handle the errors there.
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
	}
}
