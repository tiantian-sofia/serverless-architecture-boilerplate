'use strict';

const PermanentMessageError = require('./errors').PermanentMessageError;

/**
 * Event envelope:
 *
 * {
 *   "eventId": "uuid / unique id of this event",
 *   "hashKey": "book partition key",
 *   "version": 123,           // monotonic increasing per hashKey
 *   "type": "book.upsert" | "book.incrementCounter" | "book.processed",
 *   "payload": { ... type specific fields ... }
 * }
 *
 * `version` orders events for the same hashKey: a new event is applied
 * only when version > the currently stored version.
 */

const VERSION_TTL_SECONDS = 7 * 24 * 60 * 60;

const TYPES = {
    'book.upsert': {
        keys: ['title', 'author', 'price'],
        validate: function (payload) {
            if (payload.price !== undefined &&
                (typeof payload.price !== 'number' || isNaN(payload.price) || payload.price < 0)) {
                return 'price must be a non-negative number';
            }
            if (payload.title !== undefined && typeof payload.title !== 'string') {
                return 'title must be a string';
            }
            if (payload.author !== undefined && typeof payload.author !== 'string') {
                return 'author must be a string';
            }
            return null;
        }
    },
    'book.incrementCounter': {
        keys: ['counter', 'amount'],
        validate: function (payload) {
            if (typeof payload.counter !== 'string' || payload.counter.length === 0) {
                return 'counter must be a non-empty string';
            }
            if (typeof payload.amount !== 'number' || isNaN(payload.amount)) {
                return 'amount must be a number';
            }
            return null;
        }
    },
    // Preserves the original boilerplate worker behaviour (flag the row).
    'book.processed': {
        keys: [],
        validate: function () {
            return null;
        }
    }
};

function isPositiveInteger(value) {
    return typeof value === 'number' && isFinite(value) &&
        Math.floor(value) === value && value >= 0;
}

/**
 * Parse and validate the raw SQS body. Throws PermanentMessageError on
 * invalid JSON or any envelope/schema violation. Never mutates input.
 *
 * @param {String} body
 * @returns {Object} validated event envelope
 */
function parseEnvelope(body) {
    if (typeof body !== 'string' || body.length === 0) {
        throw new PermanentMessageError('INVALID_BODY', 'message body is empty');
    }

    let event;
    try {
        event = JSON.parse(body);
    } catch (err) {
        throw new PermanentMessageError('INVALID_JSON', err.message);
    }

    if (event === null || typeof event !== 'object' || Array.isArray(event)) {
        throw new PermanentMessageError('INVALID_ENVELOPE', 'body must be a JSON object');
    }

    if (typeof event.eventId !== 'string' || event.eventId.length === 0) {
        throw new PermanentMessageError('MISSING_FIELD', 'eventId is required');
    }

    if (typeof event.hashKey !== 'string' || event.hashKey.length === 0) {
        throw new PermanentMessageError('MISSING_FIELD', 'hashKey is required');
    }

    if (!isPositiveInteger(event.version)) {
        throw new PermanentMessageError('INVALID_FIELD',
            'version must be a non-negative integer');
    }

    if (typeof event.type !== 'string' || !TYPES.hasOwnProperty(event.type)) {
        throw new PermanentMessageError('UNKNOWN_TYPE',
            'type must be one of ' + Object.keys(TYPES).join(', '));
    }

    if (event.payload === null || typeof event.payload !== 'object' ||
        Array.isArray(event.payload)) {
        throw new PermanentMessageError('MISSING_FIELD', 'payload object is required');
    }

    const typeSpec = TYPES[event.type];
    const schemaError = typeSpec.validate(event.payload);
    if (schemaError) {
        throw new PermanentMessageError('INVALID_PAYLOAD', schemaError);
    }

    // Strip unknown top level keys to keep the applied event predictable.
    return {
        eventId: event.eventId,
        hashKey: event.hashKey,
        version: event.version,
        type: event.type,
        payload: typeSpec.keys.reduce(function (acc, key) {
            if (event.payload[key] !== undefined) {
                acc[key] = event.payload[key];
            }
            return acc;
        }, {})
    };
}

module.exports = {
    parseEnvelope: parseEnvelope,
    VERSION_TTL_SECONDS: VERSION_TTL_SECONDS,
    TYPES: Object.keys(TYPES)
};
