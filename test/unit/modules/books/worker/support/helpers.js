'use strict';

let seq = 0;

function messageId() {
    seq += 1;
    return 'msg-' + seq.toString().padStart(4, '0');
}

function record(event, options) {
    options = options || {};
    const body = typeof event === 'string' ? event : JSON.stringify(event);
    return {
        messageId: options.messageId || messageId(),
        receiptHandle: 'receipt-' + (options.messageId || seq),
        body: body,
        attributes: Object.assign({
            ApproximateReceiveCount: String(options.receiveCount || 1),
            SentTimestamp: '1700000000000'
        }, options.attributes || {}),
        eventSourceARN: 'arn:aws:sqs:us-east-1:123456789012:test-messages-logs',
        messageAttributes: options.messageAttributes || {}
    };
}

function envelope(overrides) {
    const base = {
        eventId: 'evt-' + (seq) + '-' + Math.floor(Math.random() * 1e6),
        hashKey: 'book-' + Math.floor(Math.random() * 1e6),
        version: 1,
        type: 'book.upsert',
        payload: { title: 't', author: 'a', price: 1 }
    };
    return Object.assign(base, overrides || {});
}

module.exports = {
    record: record,
    envelope: envelope
};
