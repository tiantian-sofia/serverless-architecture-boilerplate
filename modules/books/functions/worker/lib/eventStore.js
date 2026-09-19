'use strict';

const errors = require('./errors');
const DuplicateMessageError = errors.DuplicateMessageError;
const StaleMessageError = errors.StaleMessageError;
const RetryableError = errors.RetryableError;
const schema = require('./schema');

/**
 * Atomic idempotency + ordering + business update.
 *
 * applyEvent() executes a single DynamoDB TransactWriteItems containing:
 *
 *   1) Conditional PutItem on the idempotency ledger:
 *        ConditionExpression: attribute_not_exists(eventId)
 *      -> fails when the eventId was already applied (duplicate delivery).
 *
 *   2) Conditional UpdateItem on the books state table:
 *        ConditionExpression: attribute_not_exists(hashkey) OR #v < :newVersion
 *      -> fails when a newer/equal version is already present (stale event,
 *         including two concurrent deliveries of the same version).
 *
 * Both checks and the state mutation commit together: there is no
 * read-then-write race window. DynamoDB serializes conflicting concurrent
 * transactions and exactly one wins per (eventId, version).
 *
 * DynamoDB TransactionCanceledException carries per-request
 * CancellationReasons with ConditionalCheckFailed codes; these are mapped
 * to Duplicate / Stale outcomes rather than retries.
 */

const CONDITIONAL_CHECK_FAILED = 'ConditionalCheckFailed';
const TRANSACTION_CANCELED = 'TransactionCanceledException';
const RETRYABLE_DDB_CODES = {
    ProvisionedThroughputExceededException: true,
    ThrottlingException: true,
    RequestLimitExceeded: true,
    InternalServerError: true,
    ServiceUnavailable: true
};

function defaultDocumentClient() {
    const AWS = require('aws-sdk');
    const config = process.env.IS_OFFLINE ? {
        region: 'localhost',
        endpoint: 'http://dynamo:8000',
        accessKeyId: 'MOCK_ACCESS_KEY_ID',
        secretAccessKey: 'MOCK_SECRET_ACCESS_KEY',
        convertEmptyValues: true
    } : { region: process.env.REGION || 'us-east-1' };
    return new AWS.DynamoDB.DocumentClient(config);
}

function createEventStore(options) {
    options = options || {};
    const ddb = options.documentClient || defaultDocumentClient();
    const booksTable = options.booksTable || process.env.DYNAMO_TABLE_BOOKS;
    const idempotencyTable = options.idempotencyTable || process.env.IDEMPOTENCY_TABLE;
    const ttlSeconds = options.idempotencyTtlSeconds || schema.VERSION_TTL_SECONDS;
    const clock = options.clock || Date;

    function buildUpdate(event) {
        const update = {
            TableName: booksTable,
            Key: { hashkey: event.hashKey },
            ConditionExpression:
                'attribute_not_exists(hashkey) OR #v < :newVersion'
        };

        const sets = ['#v = :newVersion'];
        const names = { '#v': 'version' };
        const values = { ':newVersion': event.version };

        if (event.type === 'book.upsert') {
            var i = 0;
            ['title', 'author', 'price'].forEach(function (field) {
                if (event.payload[field] !== undefined) {
                    var alias = ':p' + (i++);
                    var nameAlias = '#n_' + field;
                    sets.push(nameAlias + ' = ' + alias);
                    names[nameAlias] = field;
                    values[alias] = event.payload[field];
                }
            });
        } else if (event.type === 'book.incrementCounter') {
            var counterName = '#c_' + event.payload.counter;
            names[counterName] = event.payload.counter;
            values[':amount'] = event.payload.amount;
            values[':zero'] = 0;
            sets.push(counterName +
                ' = if_not_exists(' + counterName + ', :zero) + :amount');
        } else if (event.type === 'book.processed') {
            names['#flag'] = 'updated_by_worker';
            values[':flag'] = 1;
            sets.push('#flag = :flag');
        }

        update.UpdateExpression = 'SET ' + sets.join(', ');
        update.ExpressionAttributeNames = names;
        update.ExpressionAttributeValues = values;
        return update;
    }

    function classifyCancellation(err, event) {
        const reasons = err.cancellationReasons || [];
        // Index 0: idempotency ledger put. Index 1: books state update.
        if (reasons[0] && reasons[0].Code === CONDITIONAL_CHECK_FAILED) {
            return { duplicate: true };
        }
        if (reasons[1] && reasons[1].Code === CONDITIONAL_CHECK_FAILED) {
            return { stale: true };
        }
        // No actionable reason codes - treat as a transient failure.
        return { retryable: true };
    }

    /**
     * Apply one validated event atomically.
     * Resolves on success / duplicate / stale; rejects with RetryableError
     * on transient storage failures.
     *
     * @param {Object} event validated envelope (see schema.js)
     */
    function applyEvent(event) {
        const params = {
            TransactItems: [
                {
                    Put: {
                        TableName: idempotencyTable,
                        Item: {
                            eventId: event.eventId,
                            hashKey: event.hashKey,
                            version: event.version,
                            type: event.type,
                            ttl: Math.floor(clock.now() / 1000) + ttlSeconds
                        },
                        ConditionExpression: 'attribute_not_exists(eventId)'
                    }
                },
                { Update: buildUpdate(event) }
            ]
        };

        return ddb.transactWriteItems(params).promise()
            .then(function () {
                return { outcome: 'success' };
            })
            .catch(function (err) {
                if (err && err.code === TRANSACTION_CANCELED) {
                    const outcome = classifyCancellation(err, event);
                    if (outcome.duplicate) {
                        throw new DuplicateMessageError(event.eventId);
                    }
                    if (outcome.stale) {
                        throw new StaleMessageError(event.hashKey,
                            'unknown', event.version);
                    }
                }

                if (err && (RETRYABLE_DDB_CODES[err.code] ||
                    err.retryable === true ||
                    err.name === 'TimeoutError')) {
                    throw new RetryableError(err.code || err.name, err.message);
                }

                // Unknown storage errors are retried rather than dropped.
                throw new RetryableError(err.code || err.name, err.message);
            });
    }

    return {
        applyEvent: applyEvent,
        _buildUpdate: buildUpdate
    };
}

module.exports = {
    createEventStore: createEventStore
};
