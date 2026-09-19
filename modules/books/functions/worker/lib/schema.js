'use strict';

const { PermanentMessageError } = require('./errors');

const isPlainObject = value => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

function parseBookEvent(body, message) {
    if (typeof body !== 'string') {
        throw new PermanentMessageError('Message body must be a JSON string', {
            reason: 'invalid_body_type'
        });
    }

    let payload;

    try {
        payload = JSON.parse(body);
    } catch (err) {
        throw new PermanentMessageError('Message body is not valid JSON', {
            reason: 'invalid_json'
        });
    }

    if (!isPlainObject(payload)) {
        throw new PermanentMessageError('Message payload must be a JSON object', {
            reason: 'invalid_payload_type'
        });
    }

    const normalizedHashKey = payload.hashKey !== undefined ? payload.hashKey : payload.hashkey;
    const normalizedVersion = payload.version !== undefined ? payload.version : 0;
    const eventId = payload.eventId || message.messageId;

    if (typeof eventId !== 'string' || eventId.trim() === '') {
        throw new PermanentMessageError('eventId must be a non-empty string', {
            reason: 'missing_event_id'
        });
    }

    if (typeof normalizedHashKey !== 'string' || normalizedHashKey.trim() === '') {
        throw new PermanentMessageError('hashKey must be a non-empty string', {
            reason: 'missing_hash_key'
        });
    }

    if (!Number.isSafeInteger(normalizedVersion) || normalizedVersion < 0) {
        throw new PermanentMessageError('version must be a non-negative safe integer', {
            reason: 'invalid_version'
        });
    }

    return {
        eventId,
        hashKey: normalizedHashKey,
        version: normalizedVersion
    };
}

module.exports = {
    parseBookEvent
};
