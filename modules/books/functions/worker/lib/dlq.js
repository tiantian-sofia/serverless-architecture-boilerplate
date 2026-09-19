'use strict';

const RetryableError = require('./errors').RetryableError;

/**
 * Quarantine a permanently invalid message in the DLQ.
 *
 * Poison messages (bad JSON, invalid schema) can never be processed; if we
 * simply failed them they would burn retries and then enter the DLQ with no
 * diagnostic context. Instead we forward the raw body to the DLQ with the
 * failure reason as MessageAttributes; the handler then acks them from the
 * main queue so a poison message never blocks or rolls back its batch.
 *
 * If the SendMessage itself fails (transient infrastructure error), a
 * RetryableError is thrown so the message stays on the main queue and is
 * redelivered - it is never silently dropped.
 */
function createDeadLetterQueue(options) {
    options = options || {};
    const sqs = options.sqs;
    const dlqUrl = options.dlqUrl || process.env.DLQ_QUEUE_URL;

    function stringAttr(value) {
        return {
            DataType: 'String',
            StringValue: String(value).substring(0, 256)
        };
    }

    function quarantine(record, reason, detail) {
        const attributes = {
            failureReason: stringAttr(reason),
            failureDetail: stringAttr(detail || ''),
            failedAt: stringAttr(new Date().toISOString()),
            sourceMessageId: stringAttr(record.messageId || 'unknown'),
            sourceQueue: stringAttr((record.eventSourceARN || 'unknown')
                .split(':').pop())
        };

        return sqs.sendToQueue(record.body, dlqUrl, attributes)
            .catch(function (err) {
                throw new RetryableError('DLQ_SEND_FAILED:' +
                    (err.code || err.name), err.message);
            });
    }

    return { quarantine: quarantine };
}

module.exports = { createDeadLetterQueue: createDeadLetterQueue };
