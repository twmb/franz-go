package kfake

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(28, 0, 3) }

func (c *Cluster) handleTxnOffsetCommit(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.TxnOffsetCommitRequest)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.pids.handleTxnOffsetCommit(creq) {
		return nil, nil
	}
	resp := req.ResponseKind().(*kmsg.TxnOffsetCommitResponse) // TODO CLAUDE add primitive error returning for all partitions. likely invalid group id?
	return resp, nil
}
