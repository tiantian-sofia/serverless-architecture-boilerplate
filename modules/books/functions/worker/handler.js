'use strict';

const dynamoClient = require('../../../../shared/lib/dynamo');
const sqs = require('../../../../shared/lib/sqs');
const logger = require('./lib/logger');
const { parseBookEvent } = require('./lib/schema');
const { createBookEventProcessor } = require('./lib/book-event-processor');
const {
    PermanentMessageError,
    RetryableProcessingError,
    DuplicateEventError,
    StaleEventError
} = require('./lib/errors');

const DEFAULT_TIMEOUT_SAFETY_MS = 1000;

function toPositiveInteger(value, fallback) {
    const parsed = parseInt(value, 10);

    if (Number.isSafeInteger(parsed) && parsed > 0) {
        return parsed;
    }

    return fallback;
}

function truncate(value, maxLength) {
    const stringValue = String(value);

    if (stringValue.length <= maxLength) {
        return stringValue;
    }

    return stringValue.slice(0, maxLength);
}

function stringAttribute(value) {
    return {
        DataType: 'String',
        StringValue: truncate(value, 256)
    };
}

function createWorker(options) {
    const config = Object.assign({
        dynamo: dynamoClient,
        sqs,
        processor: undefined,
        maxReceiveCount: process.env.SQS_MAX_RECEIVE_COUNT || 5,
        dlqUrl: process.env.BOOKS_DLQ_URL,
        timeoutSafetyMs: process.env.CONSUMER_TIMEOUT_SAFETY_MS || DEFAULT_TIMEOUT_SAFETY_MS
    }, options);

    const processor = config.processor || createBookEventProcessor({ dynamo: config.dynamo });
    const maxReceiveCount = toPositiveInteger(config.maxReceiveCount, 5);
    const timeoutSafetyMs = toPositiveInteger(config.timeoutSafetyMs, DEFAULT_TIMEOUT_SAFETY_MS);

    function emitOutcome(outcome, fields) {
        logger.info(`book_event_${outcome}`, Object.assign({ outcome }, fields));
        logger.metric('BookEventProcessing', 1, {
            component: 'books-consumer',
            stage: process.env.ENV || process.env.STAGE || 'unknown',
            outcome
        });
    }

    function quarantineMessage(record, error) {
        if (!config.dlqUrl) {
            return Promise.reject(new RetryableProcessingError('DLQ URL is not configured', {
                reason: 'missing_dlq_url'
            }));
        }

        return config.sqs.sendRaw({
            QueueUrl: config.dlqUrl,
            MessageBody: typeof record.body === 'string' ? record.body : JSON.stringify(record.body),
            MessageAttributes: {
                errorType: stringAttribute(error.name),
                reason: stringAttribute(error.details && error.details.reason || 'invalid_message'),
                errorMessage: stringAttribute(error.message),
                sourceMessageId: stringAttribute(record.messageId),
                approximateReceiveCount: stringAttribute(record.attributes && record.attributes.ApproximateReceiveCount || '1'),
                failureKind: stringAttribute('permanent')
            }
        }).then(() => {
            logger.metric('BookEventPermanentMessagesSentToDLQ', 1, {
                component: 'books-consumer',
                stage: process.env.ENV || process.env.STAGE || 'unknown',
                reason: error.details && error.details.reason || 'invalid_message'
            });
        });
    }

    function receiveCount(record) {
        return toPositiveInteger(record.attributes && record.attributes.ApproximateReceiveCount, 1);
    }

    function isFinalRetry(record) {
        return receiveCount(record) >= maxReceiveCount;
    }

    function processRecord(record, context) {
        const startedAt = Date.now();
        const baseFields = {
            messageId: record.messageId,
            receiveCount: receiveCount(record),
            remainingTimeMs: context.getRemainingTimeInMillis(),
            awsRequestId: context.awsRequestId,
            eventSourceARN: record.eventSourceARN
        };

        if (context.getRemainingTimeInMillis() <= timeoutSafetyMs) {
            const error = new RetryableProcessingError('Lambda invocation has insufficient time left', {
                reason: 'lambda_timeout_guard'
            });

            return Promise.resolve({
                status: 'retryable',
                record,
                error,
                willRedrive: isFinalRetry(record),
                startedAt
            });
        }

        let parsedEvent;

        return Promise.resolve()
            .then(() => {
                parsedEvent = parseBookEvent(record.body, record);
                return processor.processBookEvent(parsedEvent);
            })
            .then(() => {
                emitOutcome('success', Object.assign({}, baseFields, parsedEvent, {
                    durationMs: Date.now() - startedAt
                }));

                return { status: 'success', record, startedAt };
            })
            .catch(error => {
                const eventFields = parsedEvent || {};
                const durationMs = Date.now() - startedAt;

                if (error instanceof DuplicateEventError) {
                    emitOutcome('duplicate', Object.assign({}, baseFields, eventFields, { durationMs }));
                    return { status: 'duplicate', record, error, startedAt };
                }

                if (error instanceof StaleEventError) {
                    emitOutcome('stale', Object.assign({}, baseFields, eventFields, { durationMs }));
                    return { status: 'stale', record, error, startedAt };
                }

                if (error instanceof PermanentMessageError) {
                    return quarantineMessage(record, error)
                        .then(() => {
                            emitOutcome('permanent_failure', Object.assign({}, baseFields, eventFields, {
                                durationMs,
                                movedToDlq: true,
                                error
                            }));
                            return { status: 'permanent_failure', record, error, startedAt };
                        })
                        .catch(dlqError => {
                            const retryableError = new RetryableProcessingError('Failed to move permanent message to DLQ', {
                                reason: 'dlq_send_failed',
                                cause: dlqError.code || dlqError.name
                            });
                            logger.error('book_event_dlq_send_failed', Object.assign({}, baseFields, eventFields, {
                                durationMs,
                                error: retryableError,
                                cause: dlqError
                            }));
                            return {
                                status: 'retryable',
                                record,
                                error: retryableError,
                                willRedrive: false,
                                startedAt
                            };
                        });
                }

                const retryableError = error instanceof RetryableProcessingError ?
                    error :
                    new RetryableProcessingError(error.message, { cause: error.code || error.name });

                const willRedrive = isFinalRetry(record);
                emitOutcome('retryable_failure', Object.assign({}, baseFields, eventFields, {
                    durationMs,
                    willRedrive,
                    error: retryableError
                }));

                if (willRedrive) {
                    logger.metric('BookEventMaxReceiveReached', 1, {
                        component: 'books-consumer',
                        stage: process.env.ENV || process.env.STAGE || 'unknown',
                        reason: retryableError.details && retryableError.details.reason || 'retryable_error'
                    });
                }

                return {
                    status: 'retryable',
                    record,
                    error: retryableError,
                    willRedrive,
                    startedAt
                };
            });
    }

    function worker(event, context) {
        context.callbackWaitsForEmptyEventLoop = false;

        const records = Array.isArray(event && event.Records) ? event.Records : [];
        const startedAt = Date.now();

        return Promise.all(records.map(record => processRecord(record, context)))
            .then(results => {
                const batchItemFailures = results
                    .filter(result => result.status === 'retryable')
                    .map(result => ({ itemIdentifier: result.record.messageId }));

                const summary = results.reduce((counts, result) => {
                    counts[result.status] = (counts[result.status] || 0) + 1;
                    return counts;
                }, {});

                logger.info('books_consumer_batch_complete', {
                    awsRequestId: context.awsRequestId,
                    batchSize: records.length,
                    durationMs: Date.now() - startedAt,
                    batchItemFailures: batchItemFailures.length,
                    summary
                });

                return { batchItemFailures };
            });
    }

    return { worker };
}

module.exports.createWorker = createWorker;
module.exports.worker = (event, context) => createWorker().worker(event, context);
