package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeProducers: v0
//
// KIP-664: Describe the state of active producers on partitions.
//
// Behavior:
// * ACL: READ on TOPIC
// * Returns active transactional producers for each partition
//
// Version notes:
// * v0 only

func init() { regKey(61, 0, 0) }

func (c *Cluster) handleDescribeProducers(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DescribeProducersRequest)
	resp := req.ResponseKind().(*kmsg.DescribeProducersResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	for _, rt := range req.Topics {
		st := kmsg.NewDescribeProducersResponseTopic()
		st.Topic = rt.Topic

		if !c.allowedACL(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			for _, p := range rt.Partitions {
				sp := kmsg.NewDescribeProducersResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
			continue
		}

		t, tok := c.data.tps.gett(rt.Topic)
		for _, p := range rt.Partitions {
			sp := kmsg.NewDescribeProducersResponseTopicPartition()
			sp.Partition = p

			if !tok {
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				st.Partitions = append(st.Partitions, sp)
				continue
			}

			pd, pok := t[p]
			if !pok {
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				st.Partitions = append(st.Partitions, sp)
				continue
			}

			// Check if partition leader is this broker
			if pd.leader != creq.cc.b {
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				st.Partitions = append(st.Partitions, sp)
				continue
			}

			// Find active producers for this partition
			sp.ActiveProducers = c.describeActiveProducers(rt.Topic, p)
			st.Partitions = append(st.Partitions, sp)
		}

		resp.Topics = append(resp.Topics, st)
	}

	return resp, nil
}

// describeActiveProducers returns the active producers for a partition.
func (c *Cluster) describeActiveProducers(topic string, partition int32) []kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
	var producers []kmsg.DescribeProducersResponseTopicPartitionActiveProducer

	if c.pids.ids == nil {
		return producers
	}

	// Iterate through all producer IDs and find those with active transactions
	// that include this partition.
	for _, pidinf := range c.pids.ids {
		if !pidinf.inTx {
			continue
		}
		// Check if this producer has this partition in its transaction
		if !pidinf.txParts.checkp(topic, partition) {
			continue
		}

		ap := kmsg.NewDescribeProducersResponseTopicPartitionActiveProducer()
		ap.ProducerID = pidinf.id
		ap.ProducerEpoch = int32(pidinf.epoch)
		ap.LastTimestamp = pidinf.txStart.UnixMilli()
		ap.CoordinatorEpoch = 0 // kfake doesn't track coordinator epochs
		ap.CurrentTxnStartOffset = -1

		// Get last sequence from the pidwindow for this partition
		ap.LastSequence = -1
		if pw, ok := pidinf.windows.getp(topic, partition); ok && pw != nil {
			// seq[at] is the expected next sequence, so last sequence is seq[at] - 1
			ap.LastSequence = pw.seq[pw.at] - 1
		}

		// Try to find the first offset for this partition in the transaction
		if firstOffset, ok := pidinf.txPartFirstOffsets.getp(topic, partition); ok && firstOffset != nil {
			ap.CurrentTxnStartOffset = *firstOffset
		}

		producers = append(producers, ap)
	}

	return producers
}
