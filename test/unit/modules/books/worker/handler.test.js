'use strict';

const expect = require('chai').expect;

const helpers = require('./support/helpers');
const record = helpers.record;
const envelope = helpers.envelope;
const fakes = require('./support/fakes');
const createDynamoFake = fakes.createDynamoFake;
const createSqsFake = fakes.createSqsFake;

const eventStoreFactory =
    require('../../../../../modules/books/functions/worker/lib/eventStore')
        .createEventStore;
const dlqFactory =
    require('../../../../../modules/books/functions/worker/lib/dlq')
        .createDeadLetterQueue;
const createWorker =
    require('../../../../../modules/books/functions/worker/handler')
        .createWorker;

function setup(options) {
    options = options || {};
    const sqsFake = options.sqs || createSqsFake();
    const ddbFake = options.ddb || createDynamoFake({
        commitDelayMs: options.commitDelayMs || 0
    });
    const store = eventStoreFactory({
        documentClient: ddbFake,
        booksTable: 'books',
        idempotencyTable: 'idempotency'
    });
    const dlq = dlqFactory({ sqs: sqsFake, dlqUrl: 'dlq-url' });
    const worker = createWorker({
        eventStore: store,
        dlq: dlq,
        config: {
            maxReceiveCount: options.maxReceiveCount || 3,
            perRecordBudgetMs: options.perRecordBudgetMs || 5000,
            reservedTailMs: options.reservedTailMs || 50
        }
    });
    return {
        worker: worker,
        ddb: ddbFake,
        sqs: sqsFake
    };
}

function failedIds(response) {
    return response.batchItemFailures.map(function (f) {
        return f.itemIdentifier;
    });
}

describe('books-consumer SQS handler', function () {

    this.timeout(10000);

    it('reports only the failed message on a partial batch failure', function () {
        const ctx = setup();
        const good1 = envelope({ eventId: 'e-good-1', hashKey: 'h1',
            version: 1 });
        const bad = envelope({ eventId: 'e-bad', hashKey: 'h2', version: 1 });
        const good2 = envelope({ eventId: 'e-good-2', hashKey: 'h3',
            version: 1 });

        ctx.ddb.failTransactionFor('e-bad', 'ThrottlingException');

        const batch = {
            Records: [
                record(good1),
                record(bad), // this one hits the injected throttle
                record(good2)
            ]
        };

        return ctx.worker(batch).then(function (response) {
            expect(failedIds(response)).to.deep.equal([batch.Records[1].messageId]);
            expect(Object.keys(ctx.ddb._state.books).sort()).to.deep.equal(
                ['h1', 'h3']);
            expect(ctx.ddb._state.ledger['e-good-1']).to.exist;
            expect(ctx.ddb._state.ledger['e-good-2']).to.exist;
            expect(ctx.ddb._state.ledger['e-bad']).to.not.exist;
        });
    });

    it('does not fail the batch for dirty JSON or invalid schema and quarantines them in the DLQ', function () {
        const ctx = setup();
        const validEvent = envelope({ eventId: 'e-valid', hashKey: 'hv',
            version: 1 });
        const valid = record(validEvent);
        const dirty = record('{"eventId": "broken",', {
            messageId: 'dirty-json'
        });
        const missingField = record({ hashKey: 'h', version: 1,
            type: 'book.upsert', payload: {} }, { messageId: 'missing' });
        const badSchema = record(envelope({
            eventId: 'e-bad-schema',
            hashKey: 'hb',
            version: 1,
            type: 'book.upsert',
            payload: { price: -5 }
        }), { messageId: 'bad-schema' });
        const unknownType = record(envelope({
            eventId: 'e-unknown',
            type: 'something.weird'
        }), { messageId: 'unknown-type' });

        const batch = {
            Records: [valid, dirty, missingField, badSchema, unknownType]
        };

        return ctx.worker(batch).then(function (response) {
            // Nothing is returned for redelivery: invalid msgs are acked.
            expect(failedIds(response)).to.deep.equal([]);
            // The valid event was still applied.
            expect(ctx.ddb._state.ledger['e-valid']).to.exist;
            // Four invalid messages are in the DLQ with reasons.
            expect(ctx.sqs._messages).to.have.length(4);
            const reasons = ctx.sqs._messages.map(function (m) {
                return m.MessageAttributes.failureReason.StringValue;
            });
            expect(reasons).to.include('INVALID_JSON');
            expect(reasons).to.include('MISSING_FIELD');
            expect(reasons).to.include('INVALID_PAYLOAD');
            expect(reasons).to.include('UNKNOWN_TYPE');
            // Bodies preserved verbatim for forensics/repair.
            const bodies = ctx.sqs._messages.map(function (m) {
                return m.Body;
            });
            expect(bodies).to.include(dirty.body);
        });
    });

    it('applies a duplicated eventId exactly once (at-least-once delivery)', function () {
        const ctx = setup();
        const evt = envelope({ eventId: 'dup-id', hashKey: 'counter-book',
            version: 1, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 3 } });

        const first = ctx.worker({ Records: [record(evt, {
            messageId: 'delivery-1' })] });

        return first.then(function () {
            return ctx.worker({ Records: [record(evt, {
                messageId: 'delivery-2', receiveCount: 2 })] });
        }).then(function (response) {
            // Duplicate is acked, not retried.
            expect(failedIds(response)).to.deep.equal([]);
            // Counter only accumulated once.
            expect(ctx.ddb._state.books['counter-book'].views).to.equal(3);
            // Only one ledger row exists for the event.
            expect(Object.keys(ctx.ddb._state.ledger)).to.deep.equal(
                ['dup-id']);
        });
    });

    it('serializes concurrent processing of the same hashKey without double accumulation or lost updates', function () {
        // Two Lambda invocations process two distinct events for the same
        // hashKey simultaneously. With overlapping commit windows, the
        // conditional transaction guarantees exactly one accumulates.
        const ctx = setup({ commitDelayMs: 20 });
        const evtA = envelope({ eventId: 'cc-a', hashKey: 'hot-book',
            version: 1, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 1 } });
        const evtB = envelope({ eventId: 'cc-b', hashKey: 'hot-book',
            version: 2, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 1 } });

        return Promise.all([
            ctx.worker({ Records: [record(evtA, { messageId: 'm-a' })] }),
            ctx.worker({ Records: [record(evtB, { messageId: 'm-b' })] })
        ]).then(function (results) {
            // Both batches succeed (a stale loser is acked as stale, never
            // retried in a loop).
            expect(failedIds(results[0])).to.deep.equal([]);
            expect(failedIds(results[1])).to.deep.equal([]);
            expect(ctx.ddb._state.books['hot-book'].views).to.equal(2);
            expect(ctx.ddb._state.books['hot-book'].version).to.equal(2);
            expect(Object.keys(ctx.ddb._state.ledger).sort()).to.deep.equal(
                ['cc-a', 'cc-b']);
        });
    });

    it('applies same-version concurrent deliveries exactly once (equal version tie)', function () {
        const ctx = setup({ commitDelayMs: 20 });
        const evtA = envelope({ eventId: 'tie-a', hashKey: 'tie-book',
            version: 5, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 1 } });
        const evtB = envelope({ eventId: 'tie-b', hashKey: 'tie-book',
            version: 5, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 1 } });

        return Promise.all([
            ctx.worker({ Records: [record(evtA)] }),
            ctx.worker({ Records: [record(evtB)] })
        ]).then(function () {
            // One event applies, the other is classified stale; counter
            // never becomes 2 and nothing loops back to the queue.
            expect(ctx.ddb._state.books['tie-book'].views).to.equal(1);
        });
    });

    it('ignores older versions arriving out of order (old never overwrites new)', function () {
        const ctx = setup();
        const v3 = envelope({ eventId: 'ord-3', hashKey: 'ord-book',
            version: 3, type: 'book.upsert',
            payload: { title: 'v3-title' } });
        const v1 = envelope({ eventId: 'ord-1', hashKey: 'ord-book',
            version: 1, type: 'book.upsert',
            payload: { title: 'v1-title' } });
        const v2 = envelope({ eventId: 'ord-2', hashKey: 'ord-book',
            version: 2, type: 'book.upsert',
            payload: { title: 'v2-title' } });
        const v4 = envelope({ eventId: 'ord-4', hashKey: 'ord-book',
            version: 4, type: 'book.upsert',
            payload: { title: 'v4-title' } });

        return ctx.worker({ Records: [record(v3)] }).then(function () {
            // v1 and v2 arrive late, then v4 catches up.
            return ctx.worker({ Records: [record(v1), record(v2), record(v4)] });
        }).then(function (response) {
            expect(failedIds(response)).to.deep.equal([]);
            const book = ctx.ddb._state.books['ord-book'];
            expect(book.title).to.equal('v4-title');
            expect(book.version).to.equal(4);
        });
    });

    it('retries after a transient storage failure without partial side effects', function () {
        const ctx = setup();
        const evt = envelope({ eventId: 'retry-1', hashKey: 'r-book',
            version: 1, type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 7 } });

        ctx.ddb.failNextTransactionWith('ProvisionedThroughputExceededException');

        return ctx.worker({ Records: [record(evt, {
            messageId: 'm-retry', receiveCount: 1 })] })
            .then(function (response) {
                // First attempt: reported for redelivery, nothing applied.
                expect(failedIds(response)).to.deep.equal(['m-retry']);
                expect(ctx.ddb._state.books['r-book']).to.not.exist;
                expect(ctx.ddb._state.ledger['retry-1']).to.not.exist;

                // SQS redelivers the same message (same eventId, count=2).
                return ctx.worker({ Records: [record(evt, {
                    messageId: 'm-retry', receiveCount: 2 })] });
            })
            .then(function (response) {
                expect(failedIds(response)).to.deep.equal([]);
                expect(ctx.ddb._state.books['r-book'].views).to.equal(7);
            });
    });

    it('moves messages to the DLQ once maxReceiveCount is exhausted', function () {
        const ctx = setup({ maxReceiveCount: 3 });
        const evt = envelope({ eventId: 'poison-infra', hashKey: 'p-book',
            version: 1 });

        ctx.ddb.setFailureMode('InternalServerError');

        return ctx.worker({ Records: [record(evt, {
            messageId: 'm-exhausted', receiveCount: 3 })] })
            .then(function (response) {
                // On the final attempt it lands in the DLQ and is acked,
                // breaking the retry loop.
                expect(failedIds(response)).to.deep.equal([]);
                expect(ctx.sqs._messages).to.have.length(1);
                const dlqMsg = ctx.sqs._messages[0];
                expect(dlqMsg.QueueUrl).to.equal('dlq-url');
                expect(dlqMsg.MessageAttributes.failureReason.StringValue)
                    .to.equal('MAX_RECEIVE_COUNT_EXCEEDED');
                expect(dlqMsg.MessageAttributes.sourceMessageId.StringValue)
                    .to.equal('m-exhausted');
            });
    });

    it('returns a still-failing message for SQS redrive before the limit', function () {
        const ctx = setup({ maxReceiveCount: 5 });
        const evt = envelope({ eventId: 'still-trying', hashKey: 's-book',
            version: 1 });
        ctx.ddb.setFailureMode('ThrottlingException');

        return ctx.worker({ Records: [record(evt, {
            messageId: 'm-try', receiveCount: 2 })] })
            .then(function (response) {
                expect(failedIds(response)).to.deep.equal(['m-try']);
                expect(ctx.sqs._messages).to.have.length(0);
            });
    });

    it('keeps a message for retry when the DLQ send itself fails', function () {
        const ctx = setup();
        const invalid = record('not-json', { messageId: 'dlq-down' });
        ctx.sqs.failNextSend();

        return ctx.worker({ Records: [invalid] })
            .then(function (response) {
                // Not acked: SQS redelivers, nothing is silently dropped.
                expect(failedIds(response)).to.deep.equal(['dlq-down']);
            });
    });

    it('marks records for retry when the per-record time budget is exhausted', function () {
        const ctx = setup({
            perRecordBudgetMs: 40,
            reservedTailMs: 5,
            commitDelayMs: 100
        });
        const evt = envelope({ eventId: 'slow-1', hashKey: 'slow-book',
            version: 1 });

        return ctx.worker({ Records: [record(evt, {
            messageId: 'm-slow' })] })
            .then(function (response) {
                // The write never commits before the budget; the record is
                // redelivered (visibility timeout keeps it safe).
                expect(failedIds(response)).to.deep.equal(['m-slow']);
                expect(ctx.ddb._state.books['slow-book']).to.not.exist;
            });
    });

    it('handles an empty batch', function () {
        const ctx = setup();
        return ctx.worker({ Records: [] }).then(function (response) {
            expect(response.batchItemFailures).to.deep.equal([]);
        });
    });
});
