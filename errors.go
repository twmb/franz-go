package kgo

import (
	"errors"
)

var (
	errClientClosing         = errors.New("client closing")
	errCorrelationIDMismatch = errors.New("correlation ID mismatch")

	errRecordTooLarge = errors.New("record is too large given client max limits")

	errBrokerTooOld  = errors.New("broker is too old; this client does not support the broker")
	errNotEnoughData = errors.New("response did not contain enough data to be valid")
	errNoBrokers     = errors.New("all connections to all brokers have died")
	errInvalidResp   = errors.New("invalid response")

	errNoResp = errors.New("message was not replied to in a produce response")

	errBrokerDead           = errors.New("broker has been closed")
	errBrokerConnectionDied = errors.New("broker connection has died")
)
