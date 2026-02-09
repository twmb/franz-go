package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers = flag.String("b", "", "comma delimited brokers to consume from")
	topic   = flag.String("t", "", "topic to consume")
	group   = flag.String("g", "", "group to consume in")
)

type tp struct {
	t string
	p int32
}

type pconsumer struct {
	cl        *kgo.Client
	topic     string
	partition int32

	quit chan struct{}
	done chan struct{}
	recs chan kgo.FetchTopicPartition
}

type splitConsume struct {
	// Using BlockRebalanceOnPoll means we do not need a mu to manage
	// consumers, unlike the autocommit normal example.
	consumers map[tp]*pconsumer
}

func (pc *pconsumer) consume() {
	defer close(pc.done)
	fmt.Printf("starting, t %s p %d\n", pc.topic, pc.partition)
	defer fmt.Printf("killing, t %s p %d\n", pc.topic, pc.partition)
	for {
		select {
		case <-pc.quit:
			return
		case p := <-pc.recs:
			time.Sleep(time.Duration(rand.IntN(150)+100) * time.Millisecond) // simulate work
			fmt.Printf("Some sort of work done, about to commit t %s p %d\n", pc.topic, pc.partition)
			pc.cl.MarkCommitRecords(p.Records...)
		}
	}
}

func (s *splitConsume) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pconsumer{
				cl:        cl,
				topic:     topic,
				partition: partition,

				quit: make(chan struct{}),
				done: make(chan struct{}),
				recs: make(chan kgo.FetchTopicPartition, 5),
			}
			s.consumers[tp{topic, partition}] = pc
			go pc.consume()
		}
	}
}

func (s *splitConsume) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	s.killConsumers(revoked)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		fmt.Printf("Revoke commit failed: %v\n", err)
	}
}

func (s *splitConsume) lost(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
	s.killConsumers(lost)
	// Losing means we cannot commit: an error happened.
}

func (s *splitConsume) killConsumers(lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := tp{topic, partition}
			pc := s.consumers[tp]
			delete(s.consumers, tp)
			close(pc.quit)
			fmt.Printf("waiting for work to finish t %s p %d\n", topic, partition)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

func main() {
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
		consumers: make(map[tp]*pconsumer),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.revoked),
		kgo.OnPartitionsLost(s.lost),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()
	if err = cl.Ping(context.Background()); err != nil { // check connectivity to cluster
		panic(err)
	}

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
		// PollRecords is strongly recommended when using
		// BlockRebalanceOnPoll. You can tune how many records to
		// process at once (upper bound -- could all be on one
		// partition), ensuring that your processor loops complete fast
		// enough to not block a rebalance too long.
		fetches := cl.PollRecords(context.Background(), 10000)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			// Note: you can delete this block, which will result
			// in these errors being sent to the partition
			// consumers, and then you can handle the errors there.
			panic(err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := tp{p.Topic, p.Partition}

			// Since we are using BlockRebalanceOnPoll, we can be
			// sure this partition consumer exists:
			//
			// * onAssigned is guaranteed to be called before we
			// fetch offsets for newly added partitions
			//
			// * onRevoked waits for partition consumers to quit
			// and be deleted before re-allowing polling.
			s.consumers[tp].recs <- p
		})
		cl.AllowRebalance()
	}
}
