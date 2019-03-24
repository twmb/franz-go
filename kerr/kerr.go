// Package kerr contains Kafka errors.
//
// The errors are undocumented to avoid duplicating the official descriptions
// that can be found at http://kafka.apache.org/protocol.html#protocol_error_codes.
//
// Since this package is dedicated to errors and the package is named "kerr",
// all errors elide the standard "Err" prefix.
package kerr

// Error is a Kafka error.
type Error struct {
	// Message is the string form of a Kafka error code
	// (UNKNOWN_SERVER_ERROR, etc).
	Message string
	// Code is a Kafka error code.
	Code int16
	// Retriable is whether the error is considered retriable by Kafka.
	Retriable bool
}

func (e *Error) Error() string {
	return e.Message
}

// Code returns the error corresponding to the given error code.
//
// If the code is unknown, this returns UnknownServerError.
// If the code is 0, this returns nil.
func Code(code int16) error {
	err, exists := code2err[code]
	if !exists {
		return UnknownServerError
	}
	return err
}

var (
	UnknownServerError                 = &Error{"UNKNOWN_SERVER_ERROR", -1, false}
	OffsetOutOfRange                   = &Error{"OFFSET_OUT_OF_RANGE", 1, false}
	CorruptMessage                     = &Error{"CORRUPT_MESSAGE", 2, true}
	UnknownTopicOrPartition            = &Error{"UNKNOWN_TOPIC_OR_PARTITION", 3, true}
	InvalidFetchSize                   = &Error{"INVALID_FETCH_SIZE", 4, false}
	LeaderNotAvailable                 = &Error{"LEADER_NOT_AVAILABLE", 5, true}
	NotLeaderForPartition              = &Error{"NOT_LEADER_FOR_PARTITION", 6, true}
	RequestTimedOut                    = &Error{"REQUEST_TIMED_OUT", 7, true}
	BrokerNotAvailable                 = &Error{"BROKER_NOT_AVAILABLE", 8, false}
	ReplicaNotAvailable                = &Error{"REPLICA_NOT_AVAILABLE", 9, false}
	MessageTooLarge                    = &Error{"MESSAGE_TOO_LARGE", 10, false}
	StaleControllerEpoch               = &Error{"STALE_CONTROLLER_EPOCH", 11, false}
	OffsetMetadataTooLarge             = &Error{"OFFSET_METADATA_TOO_LARGE", 12, false}
	NetworkException                   = &Error{"NETWORK_EXCEPTION", 13, true}
	CoordinatorLoadInProgress          = &Error{"COORDINATOR_LOAD_IN_PROGRESS", 14, true}
	CoordinatorNotAvailable            = &Error{"COORDINATOR_NOT_AVAILABLE", 15, true}
	NotCoordinator                     = &Error{"NOT_COORDINATOR", 16, true}
	InvalidTopicException              = &Error{"INVALID_TOPIC_EXCEPTION", 17, false}
	RecordListTooLarge                 = &Error{"RECORD_LIST_TOO_LARGE", 18, false}
	NotEnoughReplicas                  = &Error{"NOT_ENOUGH_REPLICAS", 19, true}
	NotEnoughReplicasAfterAppend       = &Error{"NOT_ENOUGH_REPLICAS_AFTER_APPEND", 20, true}
	InvalidRequiredAcks                = &Error{"INVALID_REQUIRED_ACKS", 21, false}
	IllegalGeneration                  = &Error{"ILLEGAL_GENERATION", 22, false}
	InconsistentGroupProtocol          = &Error{"INCONSISTENT_GROUP_PROTOCOL", 23, false}
	InvalidGroupId                     = &Error{"INVALID_GROUP_ID", 24, false}
	UnknownMemberId                    = &Error{"UNKNOWN_MEMBER_ID", 25, false}
	InvalidSessionTimeout              = &Error{"INVALID_SESSION_TIMEOUT", 26, false}
	RebalanceInProgress                = &Error{"REBALANCE_IN_PROGRESS", 27, false}
	InvalidCommitOffsetSize            = &Error{"INVALID_COMMIT_OFFSET_SIZE", 28, false}
	TopicAuthorizationFailed           = &Error{"TOPIC_AUTHORIZATION_FAILED", 29, false}
	GroupAuthorizationFailed           = &Error{"GROUP_AUTHORIZATION_FAILED", 30, false}
	ClusterAuthorizationFailed         = &Error{"CLUSTER_AUTHORIZATION_FAILED", 31, false}
	InvalidTimestamp                   = &Error{"INVALID_TIMESTAMP", 32, false}
	UnsupportedSaslMechanism           = &Error{"UNSUPPORTED_SASL_MECHANISM", 33, false}
	IllegalSaslState                   = &Error{"ILLEGAL_SASL_STATE", 34, false}
	UnsupportedVersion                 = &Error{"UNSUPPORTED_VERSION", 35, false}
	TopicAlreadyExists                 = &Error{"TOPIC_ALREADY_EXISTS", 36, false}
	InvalidPartitions                  = &Error{"INVALID_PARTITIONS", 37, false}
	InvalidReplicationFactor           = &Error{"INVALID_REPLICATION_FACTOR", 38, false}
	InvalidReplicaAssignment           = &Error{"INVALID_REPLICA_ASSIGNMENT", 39, false}
	InvalidConfig                      = &Error{"INVALID_CONFIG", 40, false}
	NotController                      = &Error{"NOT_CONTROLLER", 41, true}
	InvalidRequest                     = &Error{"INVALID_REQUEST", 42, false}
	UnsupportedForMessageFormat        = &Error{"UNSUPPORTED_FOR_MESSAGE_FORMAT", 43, false}
	PolicyViolation                    = &Error{"POLICY_VIOLATION", 44, false}
	OutOfOrderSequenceNumber           = &Error{"OUT_OF_ORDER_SEQUENCE_NUMBER", 45, false}
	DuplicateSequenceNumber            = &Error{"DUPLICATE_SEQUENCE_NUMBER", 46, false}
	InvalidProducerEpoch               = &Error{"INVALID_PRODUCER_EPOCH", 47, false}
	InvalidTxnState                    = &Error{"INVALID_TXN_STATE", 48, false}
	InvalidProducerIdMapping           = &Error{"INVALID_PRODUCER_ID_MAPPING", 49, false}
	InvalidTransactionTimeout          = &Error{"INVALID_TRANSACTION_TIMEOUT", 50, false}
	ConcurrentTransactions             = &Error{"CONCURRENT_TRANSACTIONS", 51, false}
	TransactionCoordinatorFenced       = &Error{"TRANSACTION_COORDINATOR_FENCED", 52, false}
	TransactionalIdAuthorizationFailed = &Error{"TRANSACTIONAL_ID_AUTHORIZATION_FAILED", 53, false}
	SecurityDisabled                   = &Error{"SECURITY_DISABLED", 54, false}
	OperationNotAttempted              = &Error{"OPERATION_NOT_ATTEMPTED", 55, false}
	KafkaStorageError                  = &Error{"KAFKA_STORAGE_ERROR", 56, true}
	LogDirNotFound                     = &Error{"LOG_DIR_NOT_FOUND", 57, false}
	SaslAuthenticationFailed           = &Error{"SASL_AUTHENTICATION_FAILED", 58, false}
	UnknownProducerId                  = &Error{"UNKNOWN_PRODUCER_ID", 59, false}
	ReassignmentInProgress             = &Error{"REASSIGNMENT_IN_PROGRESS", 60, false}
	DelegationTokenAuthDisabled        = &Error{"DELEGATION_TOKEN_AUTH_DISABLED", 61, false}
	DelegationTokenNotFound            = &Error{"DELEGATION_TOKEN_NOT_FOUND", 62, false}
	DelegationTokenOwnerMismatch       = &Error{"DELEGATION_TOKEN_OWNER_MISMATCH", 63, false}
	DelegationTokenRequestNotAllowed   = &Error{"DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", 64, false}
	DelegationTokenAuthorizationFailed = &Error{"DELEGATION_TOKEN_AUTHORIZATION_FAILED", 65, false}
	DelegationTokenExpired             = &Error{"DELEGATION_TOKEN_EXPIRED", 66, false}
	InvalidPrincipalType               = &Error{"INVALID_PRINCIPAL_TYPE", 67, false}
	NonEmptyGroup                      = &Error{"NON_EMPTY_GROUP", 68, false}
	GroupIdNotFound                    = &Error{"GROUP_ID_NOT_FOUND", 69, false}
	FetchSessionIdNotFound             = &Error{"FETCH_SESSION_ID_NOT_FOUND", 70, true}
	InvalidFetchSessionEpoch           = &Error{"INVALID_FETCH_SESSION_EPOCH", 71, true}
	ListenerNotFound                   = &Error{"LISTENER_NOT_FOUND", 72, true}
	TopicDeletionDisabled              = &Error{"TOPIC_DELETION_DISABLED", 73, false}
	FencedLeaderEpoch                  = &Error{"FENCED_LEADER_EPOCH", 74, true}
	UnknownLeaderEpoch                 = &Error{"UNKNOWN_LEADER_EPOCH", 75, true}
	UnsupportedCompressionType         = &Error{"UNSUPPORTED_COMPRESSION_TYPE", 76, false}
)

var code2err = map[int16]error{
	-1: UnknownServerError,
	0:  nil,
	1:  OffsetOutOfRange,
	2:  CorruptMessage,
	3:  UnknownTopicOrPartition,
	4:  InvalidFetchSize,
	5:  LeaderNotAvailable,
	6:  NotLeaderForPartition,
	7:  RequestTimedOut,
	8:  BrokerNotAvailable,
	9:  ReplicaNotAvailable,
	10: MessageTooLarge,
	11: StaleControllerEpoch,
	12: OffsetMetadataTooLarge,
	13: NetworkException,
	14: CoordinatorLoadInProgress,
	15: CoordinatorNotAvailable,
	16: NotCoordinator,
	17: InvalidTopicException,
	18: RecordListTooLarge,
	19: NotEnoughReplicas,
	20: NotEnoughReplicasAfterAppend,
	21: InvalidRequiredAcks,
	22: IllegalGeneration,
	23: InconsistentGroupProtocol,
	24: InvalidGroupId,
	25: UnknownMemberId,
	26: InvalidSessionTimeout,
	27: RebalanceInProgress,
	28: InvalidCommitOffsetSize,
	29: TopicAuthorizationFailed,
	30: GroupAuthorizationFailed,
	31: ClusterAuthorizationFailed,
	32: InvalidTimestamp,
	33: UnsupportedSaslMechanism,
	34: IllegalSaslState,
	35: UnsupportedVersion,
	36: TopicAlreadyExists,
	37: InvalidPartitions,
	38: InvalidReplicationFactor,
	39: InvalidReplicaAssignment,
	40: InvalidConfig,
	41: NotController,
	42: InvalidRequest,
	43: UnsupportedForMessageFormat,
	44: PolicyViolation,
	45: OutOfOrderSequenceNumber,
	46: DuplicateSequenceNumber,
	47: InvalidProducerEpoch,
	48: InvalidTxnState,
	49: InvalidProducerIdMapping,
	50: InvalidTransactionTimeout,
	51: ConcurrentTransactions,
	52: TransactionCoordinatorFenced,
	53: TransactionalIdAuthorizationFailed,
	54: SecurityDisabled,
	55: OperationNotAttempted,
	56: KafkaStorageError,
	57: LogDirNotFound,
	58: SaslAuthenticationFailed,
	59: UnknownProducerId,
	60: ReassignmentInProgress,
	61: DelegationTokenAuthDisabled,
	62: DelegationTokenNotFound,
	63: DelegationTokenOwnerMismatch,
	64: DelegationTokenRequestNotAllowed,
	65: DelegationTokenAuthorizationFailed,
	66: DelegationTokenExpired,
	67: InvalidPrincipalType,
	68: NonEmptyGroup,
	69: GroupIdNotFound,
	70: FetchSessionIdNotFound,
	71: InvalidFetchSessionEpoch,
	72: ListenerNotFound,
	73: TopicDeletionDisabled,
	74: FencedLeaderEpoch,
	75: UnknownLeaderEpoch,
	76: UnsupportedCompressionType,
}
