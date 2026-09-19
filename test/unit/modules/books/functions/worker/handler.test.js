'use strict';

const chai = require('chai');
const expect = chai.expect;

const { createWorker } = require('../../../../../../modules/books/functions/worker/handler');
const { FakeDynamo, transactionCanceled } = require('../../../../support/fake-dynamo');
const { FakeSqs } = require('../../../../support/fake-sqs');

function createServices() {
    const dynamo = new FakeDynamo();
    const sqs = new FakeSqs();
    const workerFactory = createWorker({
        dynamo,
        sqs,
        dlqUrl: 'http://sqs/books-dlq',
        maxReceiveCount: 3
    });

    dynamo.seedBook({ hashkey: 'book-1' });
    dynamo.seedBook({ hashkey: 'book-2' });

    return { dynamo, sqs, worker: workerFactory.worker };
}

function record(messageId, body, attributes) {
    return {
        messageId,
        body,
        attributes: Object.assign({
            ApproximateReceiveCount: '1'
        }, attributes)
    };
}

function eventRecord(messageId, payload, attributes) {
    return record(messageId, JSON.stringify(payload), attributes);
}

function context() {
    return {
        remaining: 10000,
        getRemainingTimeInMillis() {
            return this.remaining;
        }
    };
}

function sqsEvent() {
    return { Records: Array.prototype.slice.call(arguments) };
}

function serviceError() {
    const error = new Error('DynamoDB unavailable');
    error.code = 'ServiceUnavailable';
    return error;
}

describe('books SQS consumer', () => {
    it('returns only failed message identifiers for partial batch failures', async () => {
        const services = createServices();

        const response = await services.worker(sqsEvent(
            eventRecord('success-1', { eventId: 'event-1', hashKey: 'book-1', version: 1 }),
            eventRecord('retry-1', { eventId: 'event-2', hashKey: 'missing-book', version: 1 }, {
                ApproximateReceiveCount: '1'
            })
        ), context());

        expect(response.batchItemFailures).to.deep.equal([{ itemIdentifier: 'retry-1' }]);
        expect(services.dynamo.tables.state['book-1'].version).to.equal(1);
        expect(services.dynamo.tables.state['missing-book']).to.equal(undefined);
    });

    it('quarantines dirty JSON and schema-invalid messages without failing the batch', async () => {
        const services = createServices();

        const response = await services.worker(sqsEvent(
            record('bad-json', '{bad-json'),
            eventRecord('missing-key', { eventId: 'event-missing', version: 1 }),
            eventRecord('bad-version', { eventId: 'event-version', hashKey: 'book-1', version: -1 })
        ), context());

        expect(response.batchItemFailures).to.deep.equal([]);
        expect(services.sqs.sentMessages).to.have.length(3);
        expect(services.sqs.sentMessages.map(message => message.MessageAttributes.reason.StringValue)).to.deep.equal([
            'invalid_json',
            'missing_hash_key',
            'invalid_version'
        ]);
        expect(services.dynamo.calls).to.have.length(0);
    });

    it('applies a duplicate eventId only once', async () => {
        const services = createServices();
        const ctx = context();

        const first = await services.worker(sqsEvent(
            eventRecord('sqs-1', { eventId: 'same-event', hashKey: 'book-1', version: 1 })
        ), ctx);

        const second = await services.worker(sqsEvent(
            eventRecord('sqs-2', { eventId: 'same-event', hashKey: 'book-1', version: 1 }, {
                ApproximateReceiveCount: '2'
            })
        ), ctx);

        expect(first.batchItemFailures).to.deep.equal([]);
        expect(second.batchItemFailures).to.deep.equal([]);
        expect(services.dynamo.calls).to.have.length(2);
        expect(services.dynamo.tables.state['book-1'].appliedEventCount).to.equal(1);
        expect(services.dynamo.tables.state['book-1'].lastEventId).to.equal('same-event');
    });

    it('serializes concurrent commits for the same hashKey without double accumulation', async () => {
        const services = createServices();
        const gates = services.dynamo.gateTransactions();
        const ctx = context();

        const first = services.worker(sqsEvent(
            eventRecord('sqs-1', { eventId: 'event-1', hashKey: 'book-1', version: 1 })
        ), ctx);
        const second = services.worker(sqsEvent(
            eventRecord('sqs-2', { eventId: 'event-2', hashKey: 'book-1', version: 2 })
        ), ctx);

        await Promise.resolve();
        expect(gates).to.have.length(2);

        gates[0].release();
        await first;
        gates[1].release();
        await second;

        expect(services.dynamo.tables.state['book-1'].appliedEventCount).to.equal(2);
        expect(services.dynamo.tables.state['book-1'].version).to.equal(2);
    });

    it('rejects the older transaction when concurrent commits finish out of order', async () => {
        const services = createServices();
        const gates = services.dynamo.gateTransactions();
        const ctx = context();

        const oldRequest = services.worker(sqsEvent(
            eventRecord('sqs-old-started-first', { eventId: 'event-old-concurrent', hashKey: 'book-1', version: 1 })
        ), ctx);
        const newRequest = services.worker(sqsEvent(
            eventRecord('sqs-new-started-second', { eventId: 'event-new-concurrent', hashKey: 'book-1', version: 2 })
        ), ctx);

        await Promise.resolve();
        expect(gates).to.have.length(2);

        gates[1].release();
        await newRequest;
        gates[0].release();
        await oldRequest;

        expect(services.dynamo.tables.state['book-1'].version).to.equal(2);
        expect(services.dynamo.tables.state['book-1'].lastEventId).to.equal('event-new-concurrent');
        expect(services.dynamo.tables.state['book-1'].appliedEventCount).to.equal(1);
    });

    it('rejects an older event after a newer one without changing state', async () => {
        const services = createServices();
        const ctx = context();

        await services.worker(sqsEvent(
            eventRecord('sqs-new', { eventId: 'event-new', hashKey: 'book-1', version: 2 })
        ), ctx);

        const staleResponse = await services.worker(sqsEvent(
            eventRecord('sqs-old', { eventId: 'event-old', hashKey: 'book-1', version: 1 })
        ), ctx);

        expect(staleResponse.batchItemFailures).to.deep.equal([]);
        expect(services.dynamo.tables.state['book-1'].version).to.equal(2);
        expect(services.dynamo.tables.state['book-1'].lastEventId).to.equal('event-new');
        expect(services.dynamo.tables.state['book-1'].appliedEventCount).to.equal(1);
    });

    it('retries safely when storage fails after the transaction has committed', async () => {
        const services = createServices();
        services.dynamo.failAfterCommit(serviceError());
        const ctx = context();

        const failed = await services.worker(sqsEvent(
            eventRecord('sqs-timeout', { eventId: 'event-timeout', hashKey: 'book-1', version: 1 })
        ), ctx);

        expect(failed.batchItemFailures).to.deep.equal([{ itemIdentifier: 'sqs-timeout' }]);
        expect(services.dynamo.tables.idempotency['event-timeout']).to.exist;
        expect(services.dynamo.tables.state['book-1'].version).to.equal(1);

        const retried = await services.worker(sqsEvent(
            eventRecord('sqs-timeout-retry', { eventId: 'event-timeout', hashKey: 'book-1', version: 1 }, {
                ApproximateReceiveCount: '2'
            })
        ), ctx);

        expect(retried.batchItemFailures).to.deep.equal([]);
        expect(services.dynamo.tables.state['book-1'].appliedEventCount).to.equal(1);
    });

    it('retries before commit and marks the last retry for SQS redrive to DLQ', async () => {
        const services = createServices();
        services.dynamo.failBeforeCommit(serviceError(), 2);
        const ctx = context();

        const first = await services.worker(sqsEvent(
            eventRecord('sqs-retry-1', { eventId: 'event-retry', hashKey: 'book-1', version: 1 }, {
                ApproximateReceiveCount: '2'
            })
        ), ctx);
        const second = await services.worker(sqsEvent(
            eventRecord('sqs-retry-2', { eventId: 'event-retry', hashKey: 'book-1', version: 1 }, {
                ApproximateReceiveCount: '3'
            })
        ), ctx);

        expect(first.batchItemFailures).to.deep.equal([{ itemIdentifier: 'sqs-retry-1' }]);
        expect(second.batchItemFailures).to.deep.equal([{ itemIdentifier: 'sqs-retry-2' }]);
        expect(services.dynamo.calls).to.have.length(2);
        expect(services.dynamo.tables.state['book-1']).to.equal(undefined);
        expect(services.dynamo.tables.idempotency['event-retry']).to.equal(undefined);
    });

    it('returns retryable failure when permanent message quarantine cannot reach DLQ', async () => {
        const services = createServices();
        services.sqs.failSend(serviceError());

        const response = await services.worker(sqsEvent(
            record('bad-json-retry', '{bad-json')
        ), context());

        expect(response.batchItemFailures).to.deep.equal([{ itemIdentifier: 'bad-json-retry' }]);
        expect(services.dynamo.calls).to.have.length(0);
    });

    it('retryable storage race resolves once the book becomes available', async () => {
        const services = createServices();
        services.dynamo.failBeforeCommit(transactionCanceled(['None', 'ConditionalCheckFailed', 'None']));

        const missing = await services.worker(sqsEvent(
            eventRecord('sqs-missing', { eventId: 'event-missing-book', hashKey: 'book-new', version: 1 })
        ), context());

        expect(missing.batchItemFailures).to.deep.equal([{ itemIdentifier: 'sqs-missing' }]);

        services.dynamo.seedBook({ hashkey: 'book-new' });
        const retried = await services.worker(sqsEvent(
            eventRecord('sqs-missing-retry', { eventId: 'event-missing-book', hashKey: 'book-new', version: 1 }, {
                ApproximateReceiveCount: '2'
            })
        ), context());

        expect(retried.batchItemFailures).to.deep.equal([]);
        expect(services.dynamo.tables.state['book-new'].version).to.equal(1);
    });

    it('stops processing a record when the Lambda timeout guard leaves no safe margin', async () => {
        const services = createServices();
        const ctx = context();
        ctx.remaining = 1000;

        const response = await services.worker(sqsEvent(
            eventRecord('sqs-timeout-guard', { eventId: 'event-timeout-guard', hashKey: 'book-1', version: 1 })
        ), ctx);

        expect(response.batchItemFailures).to.deep.equal([{ itemIdentifier: 'sqs-timeout-guard' }]);
        expect(services.dynamo.calls).to.have.length(0);
    });
});
