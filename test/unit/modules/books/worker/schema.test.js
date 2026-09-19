'use strict';

const expect = require('chai').expect;

const schema =
    require('../../../../../modules/books/functions/worker/lib/schema');
const PermanentMessageError =
    require('../../../../../modules/books/functions/worker/lib/errors')
        .PermanentMessageError;

function expectPermanent(body, reasonPrefix) {
    try {
        schema.parseEnvelope(body);
        throw new Error('expected PermanentMessageError');
    } catch (err) {
        expect(err).to.be.instanceOf(PermanentMessageError);
        if (reasonPrefix) {
            expect(err.reason).to.equal(reasonPrefix);
        }
        expect(err.retryable).to.equal(false);
    }
}

describe('worker event envelope validation', function () {

    it('accepts a valid envelope and strips unknown payload keys', function () {
        const parsed = schema.parseEnvelope(JSON.stringify({
            eventId: 'e1',
            hashKey: 'h1',
            version: 99,
            type: 'book.upsert',
            payload: { title: 't', rogue: 'x' }
        }));
        expect(parsed.payload).to.deep.equal({ title: 't' });
        expect(parsed.version).to.equal(99);
    });

    it('accepts incrementCounter events', function () {
        const parsed = schema.parseEnvelope(JSON.stringify({
            eventId: 'e2', hashKey: 'h2', version: 0,
            type: 'book.incrementCounter',
            payload: { counter: 'views', amount: -2 }
        }));
        expect(parsed.payload.amount).to.equal(-2);
    });

    it('rejects dirty JSON', function () {
        expectPermanent('{oops', 'INVALID_JSON');
    });

    it('rejects empty / non-object bodies', function () {
        expectPermanent('', 'INVALID_BODY');
        expectPermanent('[]', 'INVALID_ENVELOPE');
        expectPermanent('"hello"', 'INVALID_ENVELOPE');
    });

    it('rejects missing eventId and hashKey', function () {
        expectPermanent(JSON.stringify({
            hashKey: 'h', version: 1, type: 'book.processed', payload: {}
        }), 'MISSING_FIELD');
        expectPermanent(JSON.stringify({
            eventId: 'e', version: 1, type: 'book.processed', payload: {}
        }), 'MISSING_FIELD');
    });

    it('rejects non-integer / negative versions', function () {
        const base = { eventId: 'e', hashKey: 'h',
            type: 'book.processed', payload: {} };
        expectPermanent(JSON.stringify(Object.assign({}, base,
            { version: '1.2' })), 'INVALID_FIELD');
        expectPermanent(JSON.stringify(Object.assign({}, base,
            { version: -3 })), 'INVALID_FIELD');
    });

    it('rejects unknown event types', function () {
        expectPermanent(JSON.stringify({
            eventId: 'e', hashKey: 'h', version: 1,
            type: 'nope', payload: {}
        }), 'UNKNOWN_TYPE');
    });

    it('rejects payload schema violations', function () {
        expectPermanent(JSON.stringify({
            eventId: 'e', hashKey: 'h', version: 1,
            type: 'book.upsert', payload: { price: 'cheap' }
        }), 'INVALID_PAYLOAD');
        expectPermanent(JSON.stringify({
            eventId: 'e', hashKey: 'h', version: 1,
            type: 'book.incrementCounter',
            payload: { counter: 'views', amount: 'x' }
        }), 'INVALID_PAYLOAD');
    });
});
