'use strict';

/**
 * Error taxonomy for message processing.
 *
 * PermanentMessageError - the message itself is invalid and retrying it
 *   verbatim can never succeed (bad JSON, schema violation, unknown type).
 *   The message is quarantined in the DLQ and acked from the main queue so
 *   it cannot poison the partition.
 *
 * DuplicateMessageError - an event with the same eventId was already
 *   applied. Detected atomically via the conditional ledger write; treated
 *   as a success (acked, no double effect).
 *
 * StaleMessageError - an older version for the same hashKey arrived after
 *   a newer one (or the exact same version was already applied by another
 *   concurrent delivery). Detected atomically via the conditional state
 *   write; treated as a success (acked, no overwrite).
 *
 * RetryableError - infrastructure/transient failure (DynamoDB throttling,
 *   DLQ outage, timeout budget exhausted). The message stays in the queue
 *   (it is reported as a batch item failure) and is redriven after the
 *   visibility timeout; after maxReceiveCount SQS moves it to the DLQ.
 */

function PermanentMessageError(reason, detail) {
    Error.call(this);
    Error.captureStackTrace(this, PermanentMessageError);
    this.name = 'PermanentMessageError';
    this.reason = reason;
    this.message = reason + (detail ? ': ' + detail : '');
    this.retryable = false;
}
require('util').inherits(PermanentMessageError, Error);

function DuplicateMessageError(eventId) {
    Error.call(this);
    Error.captureStackTrace(this, DuplicateMessageError);
    this.name = 'DuplicateMessageError';
    this.eventId = eventId;
    this.message = 'duplicate eventId ' + eventId;
    this.retryable = false;
}
require('util').inherits(DuplicateMessageError, Error);

function StaleMessageError(hashKey, currentVersion, newVersion) {
    Error.call(this);
    Error.captureStackTrace(this, StaleMessageError);
    this.name = 'StaleMessageError';
    this.hashKey = hashKey;
    this.currentVersion = currentVersion;
    this.newVersion = newVersion;
    this.message = 'stale event for ' + hashKey + ' (v' + newVersion +
        ' <= current v' + currentVersion + ')';
    this.retryable = false;
}
require('util').inherits(StaleMessageError, Error);

function RetryableError(cause, detail) {
    Error.call(this);
    Error.captureStackTrace(this, RetryableError);
    this.name = 'RetryableError';
    this.cause = cause;
    this.message = 'retryable failure' + (cause ? ': ' + cause : '') +
        (detail ? ' (' + detail + ')' : '');
    this.retryable = true;
}
require('util').inherits(RetryableError, Error);

module.exports = {
    PermanentMessageError: PermanentMessageError,
    DuplicateMessageError: DuplicateMessageError,
    StaleMessageError: StaleMessageError,
    RetryableError: RetryableError
};
