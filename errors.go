package kgo

import (
	"errors"
	"fmt"
)

var (
	errClientClosing         = errors.New("client closing")
	errCorrelationIDMismatch = errors.New("correlation ID mismatch")

	errBrokerTooOld  = errors.New("broker is too old; this client does not support the broker")
	errNotEnoughData = errors.New("response did not contain enough data to be valid")
	errTooMuchData   = errors.New("response contained too much data to be valid")
	errNotVarint     = errors.New("response bytes did not contain varint where appropriate")
	errNoBrokers     = errors.New("all connections to all brokers have died")
	errInvalidResp   = errors.New("invalid response")
)

type Error struct {
	Code      int16
	Message   string
	Retriable bool
}

func (e Error) Error() string { return e.Message }

func kafkaErr(code int16) error {
	err, known := knownCodes[code]
	if known {
		return err
	}
	return fmt.Errorf("unknown error code %d", code)
}

var knownCodes = map[int16]error{
	-1: Error{-1, "UNKNOWN_SERVER_ERROR", false},
	0:  nil,
	1:  Error{1, "OFFSET_OUT_OF_RANGE", false},
	2:  Error{2, "CORRUPT_MESSAGE", true},
	3:  Error{3, "UNKNOWN_TOPIC_OR_PARTITION", true},
	4:  Error{4, "INVALID_FETCH_SIZE", false},
	5:  Error{5, "LEADER_NOT_AVAILABLE", true},
	6:  Error{6, "NOT_LEADER_FOR_PARTITION", true},
	7:  Error{7, "REQUEST_TIMED_OUT", true},
	8:  Error{8, "BROKER_NOT_AVAILABLE", false},
	9:  Error{9, "REPLICA_NOT_AVAILABLE", false},
	10: Error{10, "MESSAGE_TOO_LARGE", false},
	11: Error{11, "STALE_CONTROLLER_EPOCH", false},
	12: Error{12, "OFFSET_METADATA_TOO_LARGE", false},
	13: Error{13, "NETWORK_EXCEPTION", true},
	14: Error{14, "COORDINATOR_LOAD_IN_PROGRESS", true},
	15: Error{15, "COORDINATOR_NOT_AVAILABLE", true},
	16: Error{16, "NOT_COORDINATOR", true},
	17: Error{17, "INVALID_TOPIC_EXCEPTION", false},
	18: Error{18, "RECORD_LIST_TOO_LARGE", false},
	19: Error{19, "NOT_ENOUGH_REPLICAS", true},
	20: Error{20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", true},
	21: Error{21, "INVALID_REQUIRED_ACKS", false},
	22: Error{22, "ILLEGAL_GENERATION", false},
	23: Error{23, "INCONSISTENT_GROUP_PROTOCOL", false},
	24: Error{24, "INVALID_GROUP_ID", false},
	25: Error{25, "UNKNOWN_MEMBER_ID", false},
	26: Error{26, "INVALID_SESSION_TIMEOUT", false},
	27: Error{27, "REBALANCE_IN_PROGRESS", false},
	28: Error{28, "INVALID_COMMIT_OFFSET_SIZE", false},
	29: Error{29, "TOPIC_AUTHORIZATION_FAILED", false},
	30: Error{30, "GROUP_AUTHORIZATION_FAILED", false},
	31: Error{31, "CLUSTER_AUTHORIZATION_FAILED", false},
	32: Error{32, "INVALID_TIMESTAMP", false},
	33: Error{33, "UNSUPPORTED_SASL_MECHANISM", false},
	34: Error{34, "ILLEGAL_SASL_STATE", false},
	35: Error{35, "UNSUPPORTED_VERSION", false},
	36: Error{36, "TOPIC_ALREADY_EXISTS", false},
	37: Error{37, "INVALID_PARTITIONS", false},
	38: Error{38, "INVALID_REPLICATION_FACTOR", false},
	39: Error{39, "INVALID_REPLICA_ASSIGNMENT", false},
	40: Error{40, "INVALID_CONFIG", false},
	41: Error{41, "NOT_CONTROLLER", true},
	42: Error{42, "INVALID_REQUEST", false},
	43: Error{43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", false},
	44: Error{44, "POLICY_VIOLATION", false},
	45: Error{45, "OUT_OF_ORDER_SEQUENCE_NUMBER", false},
	46: Error{46, "DUPLICATE_SEQUENCE_NUMBER", false},
	47: Error{47, "INVALID_PRODUCER_EPOCH", false},
	48: Error{48, "INVALID_TXN_STATE", false},
	49: Error{49, "INVALID_PRODUCER_ID_MAPPING", false},
	50: Error{50, "INVALID_TRANSACTION_TIMEOUT", false},
	51: Error{51, "CONCURRENT_TRANSACTIONS", false},
	52: Error{52, "TRANSACTION_COORDINATOR_FENCED", false},
	53: Error{53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", false},
	54: Error{54, "SECURITY_DISABLED", false},
	55: Error{55, "OPERATION_NOT_ATTEMPTED", false},
	56: Error{56, "KAFKA_STORAGE_ERROR", true},
	57: Error{57, "LOG_DIR_NOT_FOUND", false},
	58: Error{58, "SASL_AUTHENTICATION_FAILED", false},
	59: Error{59, "UNKNOWN_PRODUCER_ID", false},
	60: Error{60, "REASSIGNMENT_IN_PROGRESS", false},
	61: Error{61, "DELEGATION_TOKEN_AUTH_DISABLED", false},
	62: Error{62, "DELEGATION_TOKEN_NOT_FOUND", false},
	63: Error{63, "DELEGATION_TOKEN_OWNER_MISMATCH", false},
	64: Error{64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", false},
	65: Error{65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", false},
	66: Error{66, "DELEGATION_TOKEN_EXPIRED", false},
	67: Error{67, "INVALID_PRINCIPAL_TYPE", false},
	68: Error{68, "NON_EMPTY_GROUP", false},
	69: Error{69, "GROUP_ID_NOT_FOUND", false},
	70: Error{70, "FETCH_SESSION_ID_NOT_FOUND", true},
	71: Error{71, "INVALID_FETCH_SESSION_EPOCH", true},
	72: Error{72, "LISTENER_NOT_FOUND", true},
	73: Error{73, "TOPIC_DELETION_DISABLED", false},
	74: Error{74, "FENCED_LEADER_EPOCH", true},
	75: Error{75, "UNKNOWN_LEADER_EPOCH", true},
	76: Error{76, "UNSUPPORTED_COMPRESSION_TYPE", false},
}
