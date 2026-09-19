'use strict';

const expect = require('chai').expect;

const createEventStore =
    require('../../../../../modules/books/functions/worker/lib/eventStore')
        .createEventStore;
const errors =
    require('../../../../../modules/books/functions/worker/lib/errors');

function ddbStub(executor) {
    return {
        transactWriteItems: function () {
            return {
                promise: function () {
                    return new Promise(executor);
                }
            };
        }
    };
}

function canceled(reasons) {
    const err = new Error('Transaction cancelled, precondition not met');
    err.code = 'TransactionCanceledException';
    err.name = 'TransactionCanceledException';
    err.cancellationReasons = reasons;
    return err;
}

const event = {
    eventId: 'e1',
    hashKey: 'h1',
    version: 2,
    type: 'book.processed',
    payload: {}
};

describe('eventStore cancellation classification', function () {

    it('maps a ledger conditional failure to DuplicateMessageError', function () {
        const store = createEventStore({
            documentClient: ddbStub(function (resolve, reject) {
                reject(canceled([
                    { Code: 'ConditionalCheckFailed' },
                    { Code: 'None' }
                ]));
            }),
            booksTable: 'b', idempotencyTable: 'i'
        });

        return store.applyEvent(event).then(
            function () { throw new Error('should reject'); },
            function (err) {
                expect(err).to.be.instanceOf(errors.DuplicateMessageError);
                expect(err.eventId).to.equal('e1');
            });
    });

    it('maps a state conditional failure to StaleMessageError', function () {
        const store = createEventStore({
            documentClient: ddbStub(function (resolve, reject) {
                reject(canceled([
                    { Code: 'None' },
                    { Code: 'ConditionalCheckFailed' }
                ]));
            }),
            booksTable: 'b', idempotencyTable: 'i'
        });

        return store.applyEvent(event).then(
            function () { throw new Error('should reject'); },
            function (err) {
                expect(err).to.be.instanceOf(errors.StaleMessageError);
                expect(err.hashKey).to.equal('h1');
            });
    });

    it('maps throttling to a RetryableError', function () {
        const store = createEventStore({
            documentClient: ddbStub(function (resolve, reject) {
                const err = new Error('slow down');
                err.code = 'ThrottlingException';
                reject(err);
            }),
            booksTable: 'b', idempotencyTable: 'i'
        });

        return store.applyEvent(event).then(
            function () { throw new Error('should reject'); },
            function (err) {
                expect(err).to.be.instanceOf(errors.RetryableError);
                expect(err.retryable).to.equal(true);
            });
    });

    it('builds an atomic update with both conditional checks', function () {
        const store = createEventStore({
            documentClient: ddbStub(function () {})
        });
        const spec = store._buildUpdate(event);
        expect(spec.ConditionExpression).to.contain('attribute_not_exists');
        expect(spec.ConditionExpression).to.contain('#v < :newVersion');
    });
});
