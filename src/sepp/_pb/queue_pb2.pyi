import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeadLetterCause(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEAD_LETTER_CAUSE_UNSPECIFIED: _ClassVar[DeadLetterCause]
    DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED: _ClassVar[DeadLetterCause]
    DEAD_LETTER_CAUSE_REJECTED: _ClassVar[DeadLetterCause]
    DEAD_LETTER_CAUSE_LEASE_EXPIRED: _ClassVar[DeadLetterCause]
    DEAD_LETTER_CAUSE_ADMIN: _ClassVar[DeadLetterCause]
DEAD_LETTER_CAUSE_UNSPECIFIED: DeadLetterCause
DEAD_LETTER_CAUSE_ATTEMPTS_EXHAUSTED: DeadLetterCause
DEAD_LETTER_CAUSE_REJECTED: DeadLetterCause
DEAD_LETTER_CAUSE_LEASE_EXPIRED: DeadLetterCause
DEAD_LETTER_CAUSE_ADMIN: DeadLetterCause

class PrimitiveValue(_message.Message):
    __slots__ = ("string_value", "double_value", "int_value", "bool_value")
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    string_value: str
    double_value: float
    int_value: int
    bool_value: bool
    def __init__(self, string_value: _Optional[str] = ..., double_value: _Optional[float] = ..., int_value: _Optional[int] = ..., bool_value: bool = ...) -> None: ...

class TraceContext(_message.Message):
    __slots__ = ("traceparent", "tracestate")
    TRACEPARENT_FIELD_NUMBER: _ClassVar[int]
    TRACESTATE_FIELD_NUMBER: _ClassVar[int]
    traceparent: str
    tracestate: str
    def __init__(self, traceparent: _Optional[str] = ..., tracestate: _Optional[str] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ("data", "encoding")
    DATA_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    encoding: str
    def __init__(self, data: _Optional[bytes] = ..., encoding: _Optional[str] = ...) -> None: ...

class Job(_message.Message):
    __slots__ = ("id", "job_type", "payload", "priority", "trace_context", "enqueued_at", "attempt", "max_attempts", "lease_expires_at", "custom", "scheduled_at", "queue")
    class CustomEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: PrimitiveValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[PrimitiveValue, _Mapping]] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    TRACE_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    ENQUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_AT_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    id: str
    job_type: str
    payload: Payload
    priority: int
    trace_context: TraceContext
    enqueued_at: _timestamp_pb2.Timestamp
    attempt: int
    max_attempts: int
    lease_expires_at: _timestamp_pb2.Timestamp
    custom: _containers.MessageMap[str, PrimitiveValue]
    scheduled_at: _timestamp_pb2.Timestamp
    queue: str
    def __init__(self, id: _Optional[str] = ..., job_type: _Optional[str] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ..., priority: _Optional[int] = ..., trace_context: _Optional[_Union[TraceContext, _Mapping]] = ..., enqueued_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., attempt: _Optional[int] = ..., max_attempts: _Optional[int] = ..., lease_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., custom: _Optional[_Mapping[str, PrimitiveValue]] = ..., scheduled_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., queue: _Optional[str] = ...) -> None: ...

class JobRejection(_message.Message):
    __slots__ = ("unknown_queue", "payload_too_large", "encoding_not_allowed", "job_type_not_allowed", "custom_entries_too_many", "custom_map_too_large", "custom_key_too_long", "queue_name_too_long", "job_type_name_too_long", "idempotency_key_too_long", "scheduled_too_far", "invalid_request", "queue_full")
    UNKNOWN_QUEUE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TOO_LARGE_FIELD_NUMBER: _ClassVar[int]
    ENCODING_NOT_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_NOT_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_ENTRIES_TOO_MANY_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_MAP_TOO_LARGE_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_KEY_TOO_LONG_FIELD_NUMBER: _ClassVar[int]
    QUEUE_NAME_TOO_LONG_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_NAME_TOO_LONG_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_TOO_LONG_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_TOO_FAR_FIELD_NUMBER: _ClassVar[int]
    INVALID_REQUEST_FIELD_NUMBER: _ClassVar[int]
    QUEUE_FULL_FIELD_NUMBER: _ClassVar[int]
    unknown_queue: UnknownQueue
    payload_too_large: PayloadTooLarge
    encoding_not_allowed: EncodingNotAllowed
    job_type_not_allowed: JobTypeNotAllowed
    custom_entries_too_many: CustomEntriesTooMany
    custom_map_too_large: CustomMapTooLarge
    custom_key_too_long: CustomKeyTooLong
    queue_name_too_long: QueueNameTooLong
    job_type_name_too_long: JobTypeNameTooLong
    idempotency_key_too_long: IdempotencyKeyTooLong
    scheduled_too_far: ScheduledTooFar
    invalid_request: InvalidRequest
    queue_full: QueueFull
    def __init__(self, unknown_queue: _Optional[_Union[UnknownQueue, _Mapping]] = ..., payload_too_large: _Optional[_Union[PayloadTooLarge, _Mapping]] = ..., encoding_not_allowed: _Optional[_Union[EncodingNotAllowed, _Mapping]] = ..., job_type_not_allowed: _Optional[_Union[JobTypeNotAllowed, _Mapping]] = ..., custom_entries_too_many: _Optional[_Union[CustomEntriesTooMany, _Mapping]] = ..., custom_map_too_large: _Optional[_Union[CustomMapTooLarge, _Mapping]] = ..., custom_key_too_long: _Optional[_Union[CustomKeyTooLong, _Mapping]] = ..., queue_name_too_long: _Optional[_Union[QueueNameTooLong, _Mapping]] = ..., job_type_name_too_long: _Optional[_Union[JobTypeNameTooLong, _Mapping]] = ..., idempotency_key_too_long: _Optional[_Union[IdempotencyKeyTooLong, _Mapping]] = ..., scheduled_too_far: _Optional[_Union[ScheduledTooFar, _Mapping]] = ..., invalid_request: _Optional[_Union[InvalidRequest, _Mapping]] = ..., queue_full: _Optional[_Union[QueueFull, _Mapping]] = ...) -> None: ...

class UnknownQueue(_message.Message):
    __slots__ = ("queue",)
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    queue: str
    def __init__(self, queue: _Optional[str] = ...) -> None: ...

class PayloadTooLarge(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class EncodingNotAllowed(_message.Message):
    __slots__ = ("encoding", "allowed")
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    encoding: str
    allowed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, encoding: _Optional[str] = ..., allowed: _Optional[_Iterable[str]] = ...) -> None: ...

class JobTypeNotAllowed(_message.Message):
    __slots__ = ("job_type", "allowed")
    JOB_TYPE_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    job_type: str
    allowed: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, job_type: _Optional[str] = ..., allowed: _Optional[_Iterable[str]] = ...) -> None: ...

class CustomEntriesTooMany(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class CustomMapTooLarge(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class CustomKeyTooLong(_message.Message):
    __slots__ = ("key", "limit", "actual")
    KEY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    key: str
    limit: int
    actual: int
    def __init__(self, key: _Optional[str] = ..., limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class QueueNameTooLong(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class JobTypeNameTooLong(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class IdempotencyKeyTooLong(_message.Message):
    __slots__ = ("limit", "actual")
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    limit: int
    actual: int
    def __init__(self, limit: _Optional[int] = ..., actual: _Optional[int] = ...) -> None: ...

class ScheduledTooFar(_message.Message):
    __slots__ = ("horizon", "actual")
    HORIZON_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_FIELD_NUMBER: _ClassVar[int]
    horizon: _duration_pb2.Duration
    actual: _timestamp_pb2.Timestamp
    def __init__(self, horizon: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., actual: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InvalidRequest(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class QueueFull(_message.Message):
    __slots__ = ("queue", "limit")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    queue: str
    limit: int
    def __init__(self, queue: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class EnqueueRequest(_message.Message):
    __slots__ = ("queue", "job_type", "payload", "idempotency_key", "priority", "max_attempts", "trace_context", "custom", "scheduled_at")
    class CustomEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: PrimitiveValue
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[PrimitiveValue, _Mapping]] = ...) -> None: ...
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    TRACE_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    CUSTOM_FIELD_NUMBER: _ClassVar[int]
    SCHEDULED_AT_FIELD_NUMBER: _ClassVar[int]
    queue: str
    job_type: str
    payload: Payload
    idempotency_key: str
    priority: int
    max_attempts: int
    trace_context: TraceContext
    custom: _containers.MessageMap[str, PrimitiveValue]
    scheduled_at: _timestamp_pb2.Timestamp
    def __init__(self, queue: _Optional[str] = ..., job_type: _Optional[str] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ..., idempotency_key: _Optional[str] = ..., priority: _Optional[int] = ..., max_attempts: _Optional[int] = ..., trace_context: _Optional[_Union[TraceContext, _Mapping]] = ..., custom: _Optional[_Mapping[str, PrimitiveValue]] = ..., scheduled_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("job_id", "deduplicated")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    deduplicated: bool
    def __init__(self, job_id: _Optional[str] = ..., deduplicated: bool = ...) -> None: ...

class EnqueueBatchRequest(_message.Message):
    __slots__ = ("jobs",)
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[EnqueueRequest]
    def __init__(self, jobs: _Optional[_Iterable[_Union[EnqueueRequest, _Mapping]]] = ...) -> None: ...

class EnqueueBatchResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[JobResult]
    def __init__(self, results: _Optional[_Iterable[_Union[JobResult, _Mapping]]] = ...) -> None: ...

class JobResult(_message.Message):
    __slots__ = ("success", "rejection")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REJECTION_FIELD_NUMBER: _ClassVar[int]
    success: EnqueueResponse
    rejection: JobRejection
    def __init__(self, success: _Optional[_Union[EnqueueResponse, _Mapping]] = ..., rejection: _Optional[_Union[JobRejection, _Mapping]] = ...) -> None: ...

class EnqueueAtomicResponse(_message.Message):
    __slots__ = ("success", "rejection")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REJECTION_FIELD_NUMBER: _ClassVar[int]
    success: EnqueueAtomicSuccess
    rejection: BatchValidationFailure
    def __init__(self, success: _Optional[_Union[EnqueueAtomicSuccess, _Mapping]] = ..., rejection: _Optional[_Union[BatchValidationFailure, _Mapping]] = ...) -> None: ...

class EnqueueAtomicSuccess(_message.Message):
    __slots__ = ("responses",)
    RESPONSES_FIELD_NUMBER: _ClassVar[int]
    responses: _containers.RepeatedCompositeFieldContainer[EnqueueResponse]
    def __init__(self, responses: _Optional[_Iterable[_Union[EnqueueResponse, _Mapping]]] = ...) -> None: ...

class BatchValidationFailure(_message.Message):
    __slots__ = ("errors",)
    ERRORS_FIELD_NUMBER: _ClassVar[int]
    errors: _containers.RepeatedCompositeFieldContainer[JobValidationError]
    def __init__(self, errors: _Optional[_Iterable[_Union[JobValidationError, _Mapping]]] = ...) -> None: ...

class JobValidationError(_message.Message):
    __slots__ = ("index", "rejection")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    REJECTION_FIELD_NUMBER: _ClassVar[int]
    index: int
    rejection: JobRejection
    def __init__(self, index: _Optional[int] = ..., rejection: _Optional[_Union[JobRejection, _Mapping]] = ...) -> None: ...

class ReserveRequest(_message.Message):
    __slots__ = ("queues", "wait_timeout", "lease_duration", "worker_id", "max_jobs")
    QUEUES_FIELD_NUMBER: _ClassVar[int]
    WAIT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_JOBS_FIELD_NUMBER: _ClassVar[int]
    queues: _containers.RepeatedScalarFieldContainer[str]
    wait_timeout: _duration_pb2.Duration
    lease_duration: _duration_pb2.Duration
    worker_id: str
    max_jobs: int
    def __init__(self, queues: _Optional[_Iterable[str]] = ..., wait_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., lease_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., worker_id: _Optional[str] = ..., max_jobs: _Optional[int] = ...) -> None: ...

class ReserveResponse(_message.Message):
    __slots__ = ("jobs",)
    JOBS_FIELD_NUMBER: _ClassVar[int]
    jobs: _containers.RepeatedCompositeFieldContainer[Job]
    def __init__(self, jobs: _Optional[_Iterable[_Union[Job, _Mapping]]] = ...) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("job_id", "attempt", "worker_id")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    attempt: int
    worker_id: str
    def __init__(self, job_id: _Optional[str] = ..., attempt: _Optional[int] = ..., worker_id: _Optional[str] = ...) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ("job_id",)
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    def __init__(self, job_id: _Optional[str] = ...) -> None: ...

class NackRequest(_message.Message):
    __slots__ = ("job_id", "attempt", "reason", "retry", "worker_id")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    attempt: int
    reason: str
    retry: NackRetry
    worker_id: str
    def __init__(self, job_id: _Optional[str] = ..., attempt: _Optional[int] = ..., reason: _Optional[str] = ..., retry: _Optional[_Union[NackRetry, _Mapping]] = ..., worker_id: _Optional[str] = ...) -> None: ...

class NackRetry(_message.Message):
    __slots__ = ("default", "delay", "dead_letter")
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    DELAY_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_FIELD_NUMBER: _ClassVar[int]
    default: _empty_pb2.Empty
    delay: _duration_pb2.Duration
    dead_letter: _empty_pb2.Empty
    def __init__(self, default: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., delay: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., dead_letter: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class NackResponse(_message.Message):
    __slots__ = ("job_id", "dead_lettered")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTERED_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    dead_lettered: bool
    def __init__(self, job_id: _Optional[str] = ..., dead_lettered: bool = ...) -> None: ...

class ExtendRequest(_message.Message):
    __slots__ = ("job_id", "attempt", "lease_duration", "worker_id")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    attempt: int
    lease_duration: _duration_pb2.Duration
    worker_id: str
    def __init__(self, job_id: _Optional[str] = ..., attempt: _Optional[int] = ..., lease_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., worker_id: _Optional[str] = ...) -> None: ...

class ExtendResponse(_message.Message):
    __slots__ = ("job_id", "lease_expires_at")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    lease_expires_at: _timestamp_pb2.Timestamp
    def __init__(self, job_id: _Optional[str] = ..., lease_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeadLetterRecord(_message.Message):
    __slots__ = ("job", "cause", "failed_at", "final_attempt", "last_reason")
    JOB_FIELD_NUMBER: _ClassVar[int]
    CAUSE_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    FINAL_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    LAST_REASON_FIELD_NUMBER: _ClassVar[int]
    job: Job
    cause: DeadLetterCause
    failed_at: _timestamp_pb2.Timestamp
    final_attempt: int
    last_reason: str
    def __init__(self, job: _Optional[_Union[Job, _Mapping]] = ..., cause: _Optional[_Union[DeadLetterCause, str]] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., final_attempt: _Optional[int] = ..., last_reason: _Optional[str] = ...) -> None: ...

class DrainDeadLettersRequest(_message.Message):
    __slots__ = ("queue", "max")
    QUEUE_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    queue: str
    max: int
    def __init__(self, queue: _Optional[str] = ..., max: _Optional[int] = ...) -> None: ...

class DrainDeadLettersResponse(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[DeadLetterRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[DeadLetterRecord, _Mapping]]] = ...) -> None: ...

class GetServerInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetServerInfoResponse(_message.Message):
    __slots__ = ("server_version", "supported_protocol_versions", "server_time", "restricts_encodings", "allowed_encodings", "max_payload_bytes", "max_custom_entries", "max_custom_total_bytes", "max_custom_key_bytes", "max_queue_name_bytes", "max_job_type_bytes", "max_idempotency_key_bytes", "max_schedule_horizon", "max_enqueue_batch", "max_reserve_batch", "max_reserve_queues", "max_wait_timeout", "max_lease_duration", "strict_queues", "dead_letter_retention_enabled")
    SERVER_VERSION_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_PROTOCOL_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    SERVER_TIME_FIELD_NUMBER: _ClassVar[int]
    RESTRICTS_ENCODINGS_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_ENCODINGS_FIELD_NUMBER: _ClassVar[int]
    MAX_PAYLOAD_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_CUSTOM_ENTRIES_FIELD_NUMBER: _ClassVar[int]
    MAX_CUSTOM_TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_CUSTOM_KEY_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_QUEUE_NAME_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_JOB_TYPE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_IDEMPOTENCY_KEY_BYTES_FIELD_NUMBER: _ClassVar[int]
    MAX_SCHEDULE_HORIZON_FIELD_NUMBER: _ClassVar[int]
    MAX_ENQUEUE_BATCH_FIELD_NUMBER: _ClassVar[int]
    MAX_RESERVE_BATCH_FIELD_NUMBER: _ClassVar[int]
    MAX_RESERVE_QUEUES_FIELD_NUMBER: _ClassVar[int]
    MAX_WAIT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MAX_LEASE_DURATION_FIELD_NUMBER: _ClassVar[int]
    STRICT_QUEUES_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_RETENTION_ENABLED_FIELD_NUMBER: _ClassVar[int]
    server_version: str
    supported_protocol_versions: _containers.RepeatedScalarFieldContainer[str]
    server_time: _timestamp_pb2.Timestamp
    restricts_encodings: bool
    allowed_encodings: _containers.RepeatedScalarFieldContainer[str]
    max_payload_bytes: int
    max_custom_entries: int
    max_custom_total_bytes: int
    max_custom_key_bytes: int
    max_queue_name_bytes: int
    max_job_type_bytes: int
    max_idempotency_key_bytes: int
    max_schedule_horizon: _duration_pb2.Duration
    max_enqueue_batch: int
    max_reserve_batch: int
    max_reserve_queues: int
    max_wait_timeout: _duration_pb2.Duration
    max_lease_duration: _duration_pb2.Duration
    strict_queues: bool
    dead_letter_retention_enabled: bool
    def __init__(self, server_version: _Optional[str] = ..., supported_protocol_versions: _Optional[_Iterable[str]] = ..., server_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., restricts_encodings: bool = ..., allowed_encodings: _Optional[_Iterable[str]] = ..., max_payload_bytes: _Optional[int] = ..., max_custom_entries: _Optional[int] = ..., max_custom_total_bytes: _Optional[int] = ..., max_custom_key_bytes: _Optional[int] = ..., max_queue_name_bytes: _Optional[int] = ..., max_job_type_bytes: _Optional[int] = ..., max_idempotency_key_bytes: _Optional[int] = ..., max_schedule_horizon: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_enqueue_batch: _Optional[int] = ..., max_reserve_batch: _Optional[int] = ..., max_reserve_queues: _Optional[int] = ..., max_wait_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_lease_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., strict_queues: bool = ..., dead_letter_retention_enabled: bool = ...) -> None: ...
