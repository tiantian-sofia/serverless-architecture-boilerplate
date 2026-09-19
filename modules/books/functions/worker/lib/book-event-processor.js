'use strict';

const {
    RetryableProcessingError,
    DuplicateEventError,
    StaleEventError
} = require('./errors');

const DUPLICATE_CONDITIONAL_CODE = 'ConditionalCheckFailed';
const MISSING_BOOK_CONDITIONAL_CODE = 'ConditionalCheckFailed';

function cancellationReasonAt(err, index) {
    if (!err || !Array.isArray(err.CancellationReasons)) {
        return undefined;
    }

    return err.CancellationReasons[index] && err.CancellationReasons[index].Code;
}

function createBookEventProcessor(options) {
    const config = Object.assign({
        booksTable: process.env.DYNAMO_TABLE_BOOKS || 'books',
        stateTable: process.env.BOOK_EVENT_STATE_TABLE || 'state',
        idempotencyTable: process.env.BOOK_EVENT_IDEMPOTENCY_TABLE || 'idempotency',
        dynamo: undefined
    }, options);

    if (!config.dynamo) {
        throw new Error('dynamo client is required');
    }

    function processBookEvent(event) {
        const transactItems = [{
            Put: {
                TableName: config.idempotencyTable,
                Item: {
                    eventId: event.eventId,
                    hashKey: event.hashKey,
                    version: event.version,
                    processedAt: new Date().toISOString()
                },
                ConditionExpression: 'attribute_not_exists(eventId)'
            }
        }, {
            Update: {
                TableName: config.booksTable,
                Key: { hashkey: event.hashKey },
                UpdateExpression: 'SET updated_by_worker = :processed',
                ExpressionAttributeValues: {
                    ':processed': 1
                },
                ConditionExpression: 'attribute_exists(hashkey)'
            }
        }, {
            Update: {
                TableName: config.stateTable,
                Key: { hashKey: event.hashKey },
                UpdateExpression: 'SET #version = :nextVersion, lastEventId = :eventId, processedAt = :processedAt ADD appliedEventCount :one',
                ExpressionAttributeNames: {
                    '#version': 'version'
                },
                ExpressionAttributeValues: {
                    ':nextVersion': event.version,
                    ':eventId': event.eventId,
                    ':processedAt': new Date().toISOString(),
                    ':one': 1
                },
                ConditionExpression: 'attribute_not_exists(hashKey) OR #version < :nextVersion'
            }
        }];

        return config.dynamo.transactWrite(transactItems)
            .then(() => ({ outcome: 'success' }))
            .catch(err => {
                if (err.code !== 'TransactionCanceledException') {
                    throw new RetryableProcessingError('Book event transaction failed', {
                        cause: err.code || err.name,
                        eventId: event.eventId,
                        hashKey: event.hashKey
                    });
                }

                if (cancellationReasonAt(err, 0) === DUPLICATE_CONDITIONAL_CODE) {
                    throw new DuplicateEventError('Event has already been processed', {
                        eventId: event.eventId,
                        hashKey: event.hashKey
                    });
                }

                if (cancellationReasonAt(err, 1) === MISSING_BOOK_CONDITIONAL_CODE) {
                    throw new RetryableProcessingError('Book record does not exist yet', {
                        reason: 'book_not_found',
                        eventId: event.eventId,
                        hashKey: event.hashKey
                    });
                }

                if (cancellationReasonAt(err, 2) === 'ConditionalCheckFailed') {
                    throw new StaleEventError('A newer event has already been processed', {
                        eventId: event.eventId,
                        hashKey: event.hashKey,
                        version: event.version
                    });
                }

                throw new RetryableProcessingError('Book event transaction was rejected', {
                    reasons: (err.CancellationReasons || []).map(reason => reason.Code),
                    eventId: event.eventId,
                    hashKey: event.hashKey
                });
            });
    }

    return { processBookEvent };
}

module.exports = { createBookEventProcessor };
