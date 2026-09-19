'use strict';

function conditionalError() {
    const err = new Error('Conditional Check Failed');
    err.code = 'ConditionalCheckFailedException';
    err.type = 'ConditionalCheckFailedException';
    return err;
}

function transactionCanceled(reasons) {
    const err = new Error('Transaction Canceled');
    err.code = 'TransactionCanceledException';
    err.CancellationReasons = reasons.map(code => ({ Code: code }));
    return err;
}

function applyCondition(item, condition) {
    if (!condition) {
        return true;
    }

    if (condition === 'attribute_exists(hashkey)') {
        return Boolean(item && item.hashkey !== undefined);
    }

    if (condition === 'attribute_not_exists(eventId)') {
        return item === undefined;
    }

    if (condition === 'attribute_not_exists(hashKey)') {
        return item === undefined;
    }

    const versionMatch = condition.match(/attribute_not_exists\(hashKey\) OR #version < :nextVersion/);

    if (versionMatch) {
        return item === undefined;
    }

    throw new Error(`Unsupported fake condition: ${condition}`);
}

class FakeDynamo {
    constructor(options) {
        this.options = Object.assign({
            idempotencyTable: 'idempotency',
            stateTable: 'state',
            booksTable: 'books'
        }, options);
        this.tables = {
            [this.options.idempotencyTable]: {},
            [this.options.stateTable]: {},
            [this.options.booksTable]: {}
        };
        this.calls = [];
        this.failure = null;
        this.transactionGate = null;
    }

    seedBook(book) {
        this.tables[this.options.booksTable][book.hashkey] = book;
    }

    seedIdempotent(event) {
        this.tables[this.options.idempotencyTable][event.eventId] = event;
    }

    seedState(state) {
        this.tables[this.options.stateTable][state.hashKey] = state;
    }

    failBeforeCommit(error, attempts) {
        this.failure = {
            phase: 'before',
            error,
            attempts: attempts === undefined ? 1 : attempts,
            count: 0
        };
    }

    failAfterCommit(error, attempts) {
        this.failure = {
            phase: 'after',
            error,
            attempts: attempts === undefined ? 1 : attempts,
            count: 0
        };
    }

    gateTransactions() {
        const gates = [];
        this.transactionGate = { gates };
        return gates;
    }

    transactWrite(transactItems) {
        this.calls.push(transactItems);

        const maybeFail = phase => {
            if (!this.failure || this.failure.phase !== phase) {
                return;
            }
            this.failure.count += 1;
            if (this.failure.count <= this.failure.attempts) {
                throw this.failure.error;
            }
        };

        maybeFail('before');

        const commit = () => {
            maybeFail('before');

            const idempotentItem = transactItems[0].Put;
            const bookUpdate = transactItems[1].Update;
            const stateUpdate = transactItems[2].Update;

            const idempotentExisting = this.tables[idempotentItem.TableName][idempotentItem.Item.eventId];
            const bookKey = bookUpdate.Key.hashkey;
            const bookExisting = this.tables[bookUpdate.TableName][bookKey];
            const stateKey = stateUpdate.Key.hashKey;
            const stateExisting = this.tables[stateUpdate.TableName][stateKey];

            const idempotentPass = applyCondition(idempotentExisting, idempotentItem.ConditionExpression);
            const bookPass = applyCondition(bookExisting, bookUpdate.ConditionExpression);
            const statePass = stateExisting === undefined ||
                stateExisting.version < stateUpdate.ExpressionAttributeValues[':nextVersion'];

            if (!idempotentPass || !bookPass || !statePass) {
                throw transactionCanceled([
                    idempotentPass ? 'None' : 'ConditionalCheckFailed',
                    bookPass ? 'None' : 'ConditionalCheckFailed',
                    statePass ? 'None' : 'ConditionalCheckFailed'
                ]);
            }

            this.tables[idempotentItem.TableName][idempotentItem.Item.eventId] = idempotentItem.Item;
            this.tables[bookUpdate.TableName][bookKey] = Object.assign({}, bookExisting, {
                updated_by_worker: bookUpdate.ExpressionAttributeValues[':processed']
            });

            const applied = stateExisting ? stateExisting.appliedEventCount || 0 : 0;
            this.tables[stateUpdate.TableName][stateKey] = {
                hashKey: stateKey,
                version: stateUpdate.ExpressionAttributeValues[':nextVersion'],
                lastEventId: stateUpdate.ExpressionAttributeValues[':eventId'],
                processedAt: stateUpdate.ExpressionAttributeValues[':processedAt'],
                appliedEventCount: applied + stateUpdate.ExpressionAttributeValues[':one']
            };

            maybeFail('after');

            return {};
        };

        if (this.transactionGate) {
            let release;
            const gate = new Promise(resolve => {
                release = resolve;
            });
            this.transactionGate.gates.push({ release });
            return gate.then(() => {
                try {
                    return Promise.resolve(commit());
                } catch (err) {
                    return Promise.reject(err);
                }
            });
        }

        try {
            return Promise.resolve(commit());
        } catch (err) {
            return Promise.reject(err);
        }
    }
}

module.exports = {
    FakeDynamo,
    conditionalError,
    transactionCanceled
};
