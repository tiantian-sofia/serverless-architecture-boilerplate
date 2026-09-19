'use strict';

const sqs = require('../../../../shared/lib/sqs');
const logger = require('../../../../shared/lib/logger');
const schema = require('./lib/schema');
const errors = require('./lib/errors');
const eventStoreFactory = require('./lib/eventStore').createEventStore;
const dlqFactory = require('./lib/dlq').createDeadLetterQueue;

const PER_RECORD_BUDGET_MS =
    parseInt(process.env.PER_RECORD_BUDGET_MS || '8000', 10);
const RESERVED_TAIL_MS =
    parseInt(process.env.RESERVED_TAIL_MS || '2000', 10);
const DEFAULT_MAX_RECEIVE_COUNT =
    parseInt(process.env.SQS_MAX_RECEIVE_COUNT || '5', 10);
const DEFAULT_LAMBDA_TIMEOUT_MS =
    parseInt(process.env.LAMBDA_TIMEOUT_MS ||
        (30 * 1000), 10);

/**
 * SQS-event-driven books consumer.
 *
 * Invoked by the SQS Event Source Mapping with event.Records. It returns
 * a ReportBatchItemFailures payload listing ONLY the messageIds that must
 * be redelivered; every other message is deleted by the ESM (partial batch
 * response). We never call DeleteMessage or ReceiveMessage ourselves.
 *
 * Processing order per record:
 *   1. parse + validate envelope (permanent failures -> DLQ + ack)
 *   2. per-record time budget guard (expired -> retry, never mid-write)
 *   3. atomic TransactWriteItems (idempotency ledger + conditional state)
 *   4. exhausted retries (ApproximateReceiveCount >= maxReceiveCount) on
 *      retryable errors are forwarded to the DLQ and acked to avoid loops;
 *      otherwise SQS redrives them after the visibility timeout.
 *
 * @param {Object} deps optional overrides (eventStore, dlq, clock, config),
 *                      used by tests.
 */
function createWorker(deps) {
    deps = deps || {};
    const store = deps.eventStore || eventStoreFactory();
    const dlq = deps.dlq || dlqFactory({
        sqs: sqs,
        dlqUrl: process.env.DLQ_QUEUE_URL
    });
    const clock = deps.clock || Date;
    const config = deps.config || {
        maxReceiveCount: DEFAULT_MAX_RECEIVE_COUNT,
        perRecordBudgetMs: PER_RECORD_BUDGET_MS,
        reservedTailMs: RESERVED_TAIL_MS,
        lambdaTimeoutMs: DEFAULT_LAMBDA_TIMEOUT_MS
    };

    function emit(name, dims, context) {
        logger.metric(name, 1, dims, context);
    }

    function withBudget(promise, start, label) {
        const elapsed = clock.now() - start;
        const remaining = config.perRecordBudgetMs - elapsed;
        if (remaining <= config.reservedTailMs) {
            return Promise.reject(new errors.RetryableError('BUDGET_EXHAUSTED',
                label + ' skipped before invocation'));
        }

        let timer;
        const timeout = new Promise(function (resolve, reject) {
            timer = setTimeout(function () {
                reject(new errors.RetryableError('BUDGET_EXHAUSTED',
                    label + ' exceeded per-record budget'));
            }, Math.max(0, remaining - config.reservedTailMs));
            if (typeof timer.unref === 'function') {
                timer.unref();
            }
        });

        return Promise.race([promise, timeout])
            .then(function (result) {
                clearTimeout(timer);
                return result;
            }, function (err) {
                clearTimeout(timer);
                throw err;
            });
    }

    function receiveCount(record) {
        const attrs = record.attributes || {};
        const raw = attrs.ApproximateReceiveCount;
        const count = parseInt(raw, 10);
        return isNaN(count) ? 1 : count;
    }

    function isFinalRetry(record) {
        return receiveCount(record) >= config.maxReceiveCount;
    }

    function processRecord(record, batchStart) {
        const recordStart = clock.now();
        const base = {
            messageId: record.messageId,
            receiveCount: receiveCount(record)
        };

        // Global guard: never START processing a record if the remaining
        // invocation time is shorter than the per-record budget. The record
        // is reported failed and SQS redelivers it after the visibility
        // timeout, which is configured to exceed the Lambda timeout.
        const invocationRemaining = config.lambdaTimeoutMs -
            (recordStart - batchStart);
        if (invocationRemaining <
            config.perRecordBudgetMs + config.reservedTailMs) {
            logger.warn('invocation_budget_exhausted_skip_record', {
                messageId: record.messageId,
                remainingMs: invocationRemaining
            });
            emit('Processed', { Outcome: 'timeout_skip' }, base);
            return Promise.resolve({
                status: 'retryable_failure',
                record: record
            });
        }

        // Stage 1: permanent message errors are forwarded to the DLQ and
        // acked (success from the batch's point of view).
        let event;
        try {
            event = schema.parseEnvelope(record.body);
        } catch (permanent) {
            base.hashKey = '(invalid)';
            logger.warn('permanent_message_error', Object.assign({}, base, {
                reason: permanent.reason,
                detail: permanent.message
            }));

            // Always quarantine the poison message ourselves (with the
            // failure reason attached) and ack ONLY after the SendMessage
            // succeeds. Acking without a DLQ copy would delete the message
            // permanently - SQS redrive only fires when a message is
            // returned to the queue, never when it is acked. If the
            // SendMessage fails the error is retryable: the record is
            // returned to the queue and, once maxReceiveCount is exceeded,
            // the queue's redrive policy is the final backstop.
            return withBudget(dlq.quarantine(record,
                permanent.reason, permanent.message),
                recordStart, 'dlq-quarantine')
                .then(function () {
                    emit('Processed', { Outcome: 'permanent_failure' },
                        Object.assign({}, base, {
                            reason: permanent.reason
                        }));
                    emit('MessagesToDLQ', { Reason: permanent.reason },
                        base);
                    return { status: 'permanent_failure', record: record };
                });
        }

        Object.assign(base, {
            eventId: event.eventId,
            hashKey: event.hashKey,
            version: event.version,
            type: event.type
        });

        // Stage 2: atomic idempotent apply with a hard per-record budget.
        return withBudget(store.applyEvent(event), recordStart, 'applyEvent')
            .then(function () {
                logger.info('event_applied', base);
                emit('Processed', { Outcome: 'success' }, base);
                return { status: 'success', record: record };
            })
            .catch(function (err) {
                if (err instanceof errors.DuplicateMessageError) {
                    logger.info('duplicate_event_ignored', base);
                    emit('Processed', { Outcome: 'duplicate' }, base);
                    // Ack: the effect already happened exactly once.
                    return { status: 'duplicate', record: record };
                }

                if (err instanceof errors.StaleMessageError) {
                    logger.info('stale_event_ignored', base);
                    emit('Processed', { Outcome: 'stale' }, base);
                    // Ack: a newer version already owns this hashKey state.
                    return { status: 'stale', record: record };
                }

                if (err instanceof errors.PermanentMessageError) {
                    // Defensive: schema is parsed above, but a store-level
                    // permanent error should still be quarantined.
                    logger.warn('permanent_message_error', Object.assign({},
                        base, { reason: err.reason }));
                    return withBudget(dlq.quarantine(record,
                        err.reason || 'PERMANENT', err.message),
                        recordStart, 'dlq-quarantine')
                        .then(function () {
                            emit('Processed', { Outcome: 'permanent_failure' },
                                base);
                            emit('MessagesToDLQ', { Reason: 'permanent' },
                                base);
                            return {
                                status: 'permanent_failure',
                                record: record
                            };
                        });
                }

                // Retryable infrastructure error.
                if (isFinalRetry(record)) {
                    logger.error('retryable_error_final_attempt_to_dlq',
                        Object.assign({}, base, {
                            error: err.name,
                            detail: err.message
                        }));
                    return withBudget(dlq.quarantine(record,
                        'MAX_RECEIVE_COUNT_EXCEEDED', err.name + ': ' +
                        err.message), recordStart, 'dlq-exhausted')
                        .then(function () {
                            emit('Processed', { Outcome: 'dlq' }, base);
                            emit('MessagesToDLQ',
                                { Reason: 'max_receive_count' }, base);
                            // Ack: the message now lives in the DLQ.
                            return { status: 'dlq', record: record };
                        });
                }

                logger.error('retryable_failure', Object.assign({}, base, {
                    error: err.name,
                    detail: err.message
                }));
                emit('Processed', { Outcome: 'retryable_failure' }, base);
                return { status: 'retryable_failure', record: record };
            });
    }

    function worker(event) {
        event = event || {};
        const records = event.Records || [];
        const startedAt = clock.now();

        logger.info('batch_received', {
            batchSize: records.length
        });
        logger.metric('BatchSize', records.length, { Queue: 'books' });

        const failedIds = [];

        // Sequential application keeps the per-hashKey ordering contract
        // and avoids self-inflicted hot partitions; the time budget guard
        // guarantees we never start a write that cannot finish before the
        // Lambda timeout / visibility timeout.
        return records.reduce(function (chain, record) {
            return chain.then(function () {
                return processRecord(record, startedAt)
                    .then(function (result) {
                        if (result.status === 'retryable_failure') {
                            failedIds.push(record.messageId);
                        }
                    })
                    .catch(function (err) {
                        // Safety net: any unclassified error is retried,
                        // never acked and never silently dropped.
                        logger.error('unclassified_error_marked_for_retry', {
                            messageId: record.messageId,
                            error: err.name,
                            detail: err.message
                        });
                        emit('Processed', { Outcome: 'retryable_failure' }, {
                            messageId: record.messageId
                        });
                        failedIds.push(record.messageId);
                    });
            });
        }, Promise.resolve())
            .then(function () {
                const response = {
                    batchItemFailures: failedIds.map(function (id) {
                        return { itemIdentifier: id };
                    })
                };

                logger.info('batch_complete', {
                    batchSize: records.length,
                    failed: failedIds.length,
                    durationMs: clock.now() - startedAt
                });
                return response;
            });
    }

    return worker;
}

module.exports.worker = function (event, context) {
    if (context) {
        // Do not let the event loop (e.g. SDK keep-alive sockets) delay
        // the freeze after the response is returned.
        context.callbackWaitsForEmptyEventLoop = false;
    }
    return createWorker()(event, context);
};

module.exports.createWorker = createWorker;
