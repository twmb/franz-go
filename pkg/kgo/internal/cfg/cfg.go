// Package cfg provides a basic config struct that is used throughout the
// client. The intent is to be initialized once and then never modified again.
package cfg

import "time"

type Cfg struct {
	TxnID        *string
	ClientID     *string
	RetryBackoff func(int) time.Duration

	RecordTimeout       time.Duration
	Linger              time.Duration
	Acks                int16
	ProduceTimeout      time.Duration
	BrokerMaxWriteBytes int32
	MaxBatchBytes       int32
	ProduceRetries      int
	StopOnDataLoss      bool
	OnDataLoss          func(string, int32)
}
