package kfake

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFaultObserveAndWhen(t *testing.T) {
	t.Parallel()
	const topic = "fault-observe"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	// The observer goes in first: a request hits every fault it matches,
	// and only the erroring one answers.
	seen := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Observe: true, Count: -1})
	acks1 := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Err:   kerr.PolicyViolation,
		Count: -1,
		When:  func(kreq kmsg.Request) bool { return kreq.(*kmsg.ProduceRequest).Acks == 1 },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	all := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	if err := all.ProduceSync(ctx, &kgo.Record{Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("acks=all produce: %v", err)
	}
	one := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	if err := one.ProduceSync(ctx, &kgo.Record{Value: []byte("v")}).FirstErr(); !errors.Is(err, kerr.PolicyViolation) {
		t.Fatalf("acks=1 produce: got %v, want POLICY_VIOLATION", err)
	}

	if n := acks1.Hits(); n != 1 {
		t.Errorf("When faulted %d produces, want only the acks=1 one", n)
	}
	if n := seen.Hits(); n != 2 {
		t.Errorf("observed %d produces, want 2", n)
	}
	if err := seen.Wait(ctx, 2); err != nil {
		t.Errorf("Wait on an observing fault: %v", err)
	}
}

// An observing fault leaves the request alone, so a share fetch with nothing
// to return still waits out its MaxWait.
func TestFaultObserveLongPoll(t *testing.T) {
	t.Parallel()
	const (
		topic = "fault-observe-poll"
		group = "fault-observe-poll-g"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	memberID, topicID := joinShareGroupRaw(t, cl, group, topic)

	// Every share fetch matches this at the group check, which is where a
	// faulted share fetch is answered from.
	seen := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ShareFetch}, Observe: true, Count: -1})

	req := kmsg.NewPtrShareFetchRequest()
	req.GroupID = kmsg.StringPtr(group)
	req.MemberID = &memberID
	req.MaxWaitMillis = 2000
	req.MaxRecords = 500
	rt := kmsg.NewShareFetchRequestTopic()
	rt.TopicID = topicID
	rp := kmsg.NewShareFetchRequestTopicPartition()
	rp.PartitionMaxBytes = 1 << 20
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	start := time.Now()
	resp := faultReq(t, cl, 0, req).(*kmsg.ShareFetchResponse)
	took := time.Since(start)
	if resp.ErrorCode != 0 {
		t.Fatalf("share fetch answered %d", resp.ErrorCode)
	}
	if took < time.Second {
		t.Errorf("empty share fetch returned after %s, want the full 2s MaxWait", took)
	}
	if n := seen.Hits(); n != 1 {
		t.Errorf("observed %d share fetches != 1: a parked request is one request", n)
	}

	// Same shape on the classic fetch path, which parks on MinBytes.
	fetches := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch}, Observe: true, Count: -1})
	freq := kmsg.NewPtrFetchRequest()
	freq.MaxWaitMillis = 1000
	freq.MinBytes = 1
	freq.SessionEpoch = -1
	frt := kmsg.NewFetchRequestTopic()
	frt.Topic, frt.TopicID = topic, topicID
	frp := kmsg.NewFetchRequestTopicPartition()
	frp.CurrentLeaderEpoch = -1
	frp.PartitionMaxBytes = 1 << 20
	frt.Partitions = append(frt.Partitions, frp)
	freq.Topics = append(freq.Topics, frt)

	start = time.Now()
	faultReq(t, cl, 0, freq)
	if took := time.Since(start); took < 500*time.Millisecond {
		t.Errorf("empty fetch returned after %s, want the full 1s MaxWait", took)
	}
	if n := fetches.Hits(); n != 1 {
		t.Errorf("observed %d fetches != 1: a parked request is one request", n)
	}
}

func TestTopicLifecycle(t *testing.T) {
	t.Parallel()
	const topic = "lifecycle"
	c := newCluster(t, NumBrokers(1))

	if err := c.CreateTopic(topic, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	if err := c.CreateTopic(topic, 1, nil); err == nil {
		t.Fatal("creating a topic twice did not error")
	}
	ti := c.TopicInfo(topic)
	if v := ti.Configs["cleanup.policy"]; v == nil || *v != "compact" {
		t.Fatalf("topic configs %v do not carry cleanup.policy", ti.Configs)
	}
	first := ti.TopicID

	produceN(t, c, topic, 5)
	if err := c.DeleteRecords(topic, 0, 99); err == nil {
		t.Fatal("deleting past the high watermark did not error")
	}
	if err := c.DeleteRecords(topic, 0, -1); err != nil {
		t.Fatal(err)
	}
	if pi := c.PartitionInfo(topic, 0); pi.LogStartOffset != 5 {
		t.Fatalf("log start offset is %d after deleting every record, want 5", pi.LogStartOffset)
	}

	if err := c.DeleteTopic(topic); err != nil {
		t.Fatal(err)
	}
	if c.TopicInfo(topic) != nil {
		t.Fatal("the topic still exists after being deleted")
	}
	if err := c.DeleteTopic(topic); err == nil {
		t.Fatal("deleting a topic twice did not error")
	}
	if err := c.CreateTopic(topic, 1, nil); err != nil {
		t.Fatal(err)
	}
	if c.TopicInfo(topic).TopicID == first {
		t.Fatal("the recreated topic kept its topic ID")
	}
}

func TestGroupInfoAndWaits(t *testing.T) {
	t.Parallel()
	const (
		topic = "group-info"
		group = "group-info-g"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	produceN(t, c, topic, 4)

	if c.GroupInfo(group) != nil {
		t.Fatal("a group that does not exist has info")
	}

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	collectRecords(t, cl, 4, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}

	g, err := c.WaitGroupStable(ctx, group, 1)
	if err != nil {
		t.Fatal(err)
	}
	if g.NumAssigned() != 2 {
		t.Fatalf("group owns %d partitions, want 2", g.NumAssigned())
	}
	if len(g.Members) != 1 || g.Members[0].NumAssigned() != 2 {
		t.Fatalf("members %+v", g.Members)
	}
	if !slices.Contains(g.Members[0].SubscribedTopics, topic) {
		t.Fatalf("member subscribed to %v, want %s", g.Members[0].SubscribedTopics, topic)
	}
	if len(g.Commits[topic]) == 0 {
		t.Fatalf("group has no commits for %s", topic)
	}

	// Everything comes back copied, so writing to it does not reach the
	// group.
	for _, ps := range g.Commits {
		clear(ps)
	}
	clear(g.Commits)
	clear(g.Members[0].Assignment)
	if g2 := c.GroupInfo(group); len(g2.Commits[topic]) == 0 || g2.NumAssigned() != 2 {
		t.Fatal("GroupInfo handed back the group's own maps")
	}

	// A group that empties out is still described, so WaitGroupInfo can
	// wait for the member to leave.
	cl.Close()
	if _, err := c.WaitGroupInfo(ctx, group, func(g *GroupInfo) bool {
		return g == nil || len(g.Members) == 0
	}); err != nil {
		t.Fatalf("group never emptied: %v", err)
	}
}
