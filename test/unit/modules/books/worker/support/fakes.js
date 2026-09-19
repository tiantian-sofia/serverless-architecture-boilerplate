'use strict';

/**
 * In-memory fakes that mimic the DynamoDB DocumentClient
 * TransactWriteItems semantics and the SQS publish client used by the
 * worker. They implement exactly the conditional rules the real
 * transaction relies on, with an injectable commit delay so concurrent
 * processing of the same hashKey can be deterministically exercised.
 */

function ConditionalCheckFailed(message) {
    const err = new Error(message);
    err.code = 'TransactionCanceledException';
    err.name = 'TransactionCanceledException';
    err.cancellationReasons = [];
    return err;
}

function createDynamoFake(options) {
    options = options || {};

    const state = {
        books: options.booksState || {},
        ledger: options.ledgerState || {}
    };
    const dlq = options.dlq; // messages moved to the DLQ
    let commitDelayMs = options.commitDelayMs || 0;
    let failureMode = options.failureMode || null;
    let failOnceCode = options.failOnceCode || null;
    let failForEventId = null;
    let failForCode = null;

    function failEvent(store, key, code) {
        const err = ConditionalCheckFailed('conditional check failed');
        if (store === 'ledger') {
            err.cancellationReasons = [
                { Code: 'ConditionalCheckFailed' },
                { Code: 'None' }
            ];
        } else {
            err.cancellationReasons = [
                { Code: 'None' },
                { Code: 'ConditionalCheckFailed' }
            ];
        }
        return err;
    }

    function resolveName(exprNames, token) {
        return exprNames ? exprNames[token] : token;
    }

    function applyUpdate(item, update) {
        const names = update.ExpressionAttributeNames || {};
        const values = update.ExpressionAttributeValues || {};

        // First-write condition
        const exists = Object.keys(item).length > 0;
        if (exists && item.version >= values[':newVersion']) {
            return false; // stale
        }

        // Split SET clauses on top-level commas only (commas inside
        // function arguments must not break the split).
        const expr = update.UpdateExpression.replace(/^SET\s+/i, '');
        const clauses = [];
        let depth = 0;
        let current = '';
        for (let i = 0; i < expr.length; i++) {
            const ch = expr[i];
            if (ch === '(') depth += 1;
            if (ch === ')') depth -= 1;
            if (ch === ',' && depth === 0) {
                clauses.push(current);
                current = '';
            } else {
                current += ch;
            }
        }
        if (current.trim()) clauses.push(current);

        clauses.forEach(function (rawClause) {
            const clause = rawClause.trim();
            const eqIndex = clause.indexOf('=');
            const leftToken = clause.slice(0, eqIndex).trim();
            const rhs = clause.slice(eqIndex + 1).trim();
            const field = resolveName(names, leftToken);

            if (/if_not_exists\s*\(/i.test(rhs)) {
                // #c = if_not_exists(#c, :zero) + :amount
                const m = rhs.match(/if_not_exists\(\s*([^,]+),\s*([^)]+)\)\s*\+\s*([\s\S]+)/i);
                const counterField = resolveName(names, m[1].trim());
                const zeroToken = m[2].trim();
                const amountToken = m[3].trim();
                const zero = zeroToken.charAt(0) === ':' ? values[zeroToken] : 0;
                const amount = amountToken.charAt(0) === ':' ? values[amountToken] : Number(amountToken);
                item[counterField] = (item[counterField] === undefined ?
                    zero : item[counterField]) + amount;
            } else {
                item[field] = values[rhs.trim()];
            }
        });
        return true;
    }

    const client = {
        _state: state,

        setCommitDelay: function (ms) { commitDelayMs = ms; },
        setFailureMode: function (mode) { failureMode = mode; },
        failNextTransactionWith: function (code) { failOnceCode = code; },
        failTransactionFor: function (eventId, code) {
            failForEventId = eventId;
            failForCode = code;
        },

        transactWriteItems: function (params) {
            return {
                promise: function () {
                    return new Promise(function (resolve, reject) {
                        setTimeout(function () {
                            if (failOnceCode) {
                                const code = failOnceCode;
                                failOnceCode = null;
                                const err = new Error('injected ' + code);
                                err.code = code;
                                err.name = code;
                                return reject(err);
                            }
                            const putItem = params.TransactItems[0].Put;
                            const updateSpec = params.TransactItems[1].Update;
                            const key = updateSpec.Key.hashkey;

                            if (failForEventId &&
                                putItem.Item.eventId === failForEventId) {
                                failForEventId = null;
                                const err3 = new Error('injected event failure');
                                err3.code = failForCode;
                                err3.name = failForCode;
                                return reject(err3);
                            }
                            if (failureMode) {
                                const err = new Error('injected ' +
                                    failureMode);
                                err.code = failureMode;
                                err.name = failureMode;
                                return reject(err);
                            }

                            // Ledger condition: attribute_not_exists(eventId)
                            if (state.ledger[putItem.Item.eventId]) {
                                return reject(failEvent('ledger', key));
                            }

                            // State condition + mutation, atomically.
                            const item = state.books[key] || {};
                            const applied = applyUpdate(item, updateSpec);
                            if (!applied) {
                                return reject(failEvent('state', key));
                            }
                            item.hashkey = key;
                            state.books[key] = item;

                            // Ledger commit (records what was applied).
                            state.ledger[putItem.Item.eventId] = putItem.Item;

                            return resolve({});
                        }, commitDelayMs);
                    });
                }
            };
        }
    };

    return client;
}

function createSqsFake() {
    const messages = [];
    const failures = { send: 0 };

    return {
        _messages: messages,
        _failures: failures,
        failNextSend: function () { failures.send += 1; },

        sendToQueue: function (message, queue, attributes) {
            return new Promise(function (resolve, reject) {
                setImmediate(function () {
                    if (failures.send > 0) {
                        failures.send -= 1;
                        const err = new Error('sqs unavailable');
                        err.code = 'ServiceUnavailable';
                        err.name = 'ServiceUnavailable';
                        return reject(err);
                    }
                    messages.push({
                        QueueUrl: queue,
                        Body: typeof message === 'string' ? message :
                            JSON.stringify(message),
                        MessageAttributes: attributes
                    });
                    resolve({ MessageId: 'dlq-msg-' + messages.length });
                });
            });
        }
    };
}

module.exports = {
    createDynamoFake: createDynamoFake,
    createSqsFake: createSqsFake
};
