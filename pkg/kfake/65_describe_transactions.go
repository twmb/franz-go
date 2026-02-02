package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeTransactions: v0
//
// KIP-664: Describe the state of transactions by transactional ID.
//
// Behavior:
// * ACL: DESCRIBE on TRANSACTIONAL_ID
// * Returns transaction state, timeout, producer ID/epoch, and partitions
//
// Version notes:
// * v0 only

func init() { regKey(65, 0, 0) }

func (c *Cluster) handleDescribeTransactions(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DescribeTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.DescribeTransactionsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	for _, txnID := range req.TransactionalIDs {
		st := kmsg.NewDescribeTransactionsResponseTransactionState()
		st.TransactionalID = txnID

		// ACL check: DESCRIBE on TransactionalId
		if !c.allowedACL(creq, txnID, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe) {
			st.ErrorCode = kerr.TransactionalIDAuthorizationFailed.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		// Check if this broker is the coordinator for this txn ID
		coordinator := c.coordinator(txnID)
		if coordinator != creq.cc.b {
			st.ErrorCode = kerr.NotCoordinator.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		// Find the producer info for this transactional ID
		pidinf := c.findProducerByTxnID(txnID)
		if pidinf == nil {
			st.ErrorCode = kerr.TransactionalIDNotFound.Code
			resp.TransactionStates = append(resp.TransactionStates, st)
			continue
		}

		st.ProducerID = pidinf.id
		st.ProducerEpoch = pidinf.epoch
		st.TimeoutMillis = pidinf.txTimeout

		if pidinf.inTx {
			st.State = "Ongoing"
			st.StartTimestamp = pidinf.txStart.UnixMilli()

			// Add partitions in the transaction (only topics user can describe)
			pidinf.txParts.each(func(topic string, partition int32, _ *partData) {
				// ACL check: DESCRIBE on Topic
				if !c.allowedACL(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe) {
					return
				}

				// Find or create topic entry
				var topicEntry *kmsg.DescribeTransactionsResponseTransactionStateTopic
				for i := range st.Topics {
					if st.Topics[i].Topic == topic {
						topicEntry = &st.Topics[i]
						break
					}
				}
				if topicEntry == nil {
					st.Topics = append(st.Topics, kmsg.NewDescribeTransactionsResponseTransactionStateTopic())
					topicEntry = &st.Topics[len(st.Topics)-1]
					topicEntry.Topic = topic
				}
				topicEntry.Partitions = append(topicEntry.Partitions, partition)
			})
		} else {
			st.State = "Empty"
			st.StartTimestamp = -1
		}

		resp.TransactionStates = append(resp.TransactionStates, st)
	}

	return resp, nil
}

// findProducerByTxnID finds a producer info by transactional ID.
func (c *Cluster) findProducerByTxnID(txnID string) *pidinfo {
	if c.pids.ids == nil {
		return nil
	}
	for _, pidinf := range c.pids.ids {
		if pidinf.txid == txnID {
			return pidinf
		}
	}
	return nil
}
