package kfake

import (
	"regexp"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListTransactions: v0-2
//
// KIP-664: List transactions, optionally filtered by state or producer ID.
//
// Behavior:
// * ACL: DESCRIBE on TRANSACTIONAL_ID (filters results to only those allowed)
// * Returns list of transactions with their state
//
// Version notes:
// * v0: Initial version
// * v1: DurationFilterMillis
// * v2: TransactionalIDPattern (regex filter)

func init() { regKey(66, 0, 2) }

func (c *Cluster) handleListTransactions(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ListTransactionsRequest)
	resp := req.ResponseKind().(*kmsg.ListTransactionsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Build state filter set
	stateFilter := make(map[string]struct{})
	for _, s := range req.StateFilters {
		// Validate state
		switch s {
		case "Empty", "Ongoing", "PrepareCommit", "PrepareAbort",
			"CompleteCommit", "CompleteAbort", "Dead", "PrepareEpochFence":
			stateFilter[s] = struct{}{}
		default:
			resp.UnknownStateFilters = append(resp.UnknownStateFilters, s)
		}
	}

	// Build producer ID filter set
	pidFilter := make(map[int64]struct{})
	for _, pid := range req.ProducerIDFilters {
		pidFilter[pid] = struct{}{}
	}

	// Compile transactional ID pattern if provided (v2+)
	var txnIDRegex *regexp.Regexp
	if req.Version >= 2 && req.TransactionalIDPattern != nil && *req.TransactionalIDPattern != "" {
		var err error
		txnIDRegex, err = regexp.Compile(*req.TransactionalIDPattern)
		if err != nil {
			resp.ErrorCode = kerr.InvalidRegularExpression.Code
			return resp, nil
		}
	}

	if c.pids.ids == nil {
		return resp, nil
	}

	// Iterate through all transactional producers
	for _, pidinf := range c.pids.ids {
		// Only include transactional producers
		if pidinf.txid == "" {
			continue
		}

		// Check ACL: DESCRIBE on TransactionalId
		if !c.allowedACL(creq, pidinf.txid, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationDescribe) {
			continue
		}

		// Determine state
		var state string
		if pidinf.inTx {
			state = "Ongoing"
		} else {
			state = "Empty"
		}

		// Apply state filter
		if len(stateFilter) > 0 {
			if _, ok := stateFilter[state]; !ok {
				continue
			}
		}

		// Apply producer ID filter
		if len(pidFilter) > 0 {
			if _, ok := pidFilter[pidinf.id]; !ok {
				continue
			}
		}

		// Apply duration filter (v1+)
		if req.Version >= 1 && req.DurationFilterMillis >= 0 && pidinf.inTx {
			durationMs := creq.at.Sub(pidinf.txStart).Milliseconds()
			if durationMs < req.DurationFilterMillis {
				continue
			}
		}

		// Apply transactional ID pattern filter (v2+)
		if txnIDRegex != nil {
			if !txnIDRegex.MatchString(pidinf.txid) {
				continue
			}
		}

		ts := kmsg.NewListTransactionsResponseTransactionState()
		ts.TransactionalID = pidinf.txid
		ts.ProducerID = pidinf.id
		ts.TransactionState = state
		resp.TransactionStates = append(resp.TransactionStates, ts)
	}

	return resp, nil
}
