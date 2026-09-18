'use strict';

/**
 * Opaque pagination cursor helpers built on top of the
 * DynamoDB LastEvaluatedKey / ExclusiveStartKey payloads.
 *
 * The cursor is the JSON serialized key encoded as base64url,
 * so no per-page server state is required.
 */

function toBase64Url(value) {
    return Buffer.from(value)
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
}

function fromBase64Url(value) {
    const base64 = value
        .replace(/-/g, '+')
        .replace(/_/g, '/')
        + '==='.slice((value.length + 3) % 4);

    return Buffer.from(base64, 'base64').toString('utf8');
}

module.exports.encode = key => {
    return toBase64Url(JSON.stringify(key));
};

module.exports.decode = cursor => {
    const key = JSON.parse(fromBase64Url(cursor));

    if (!key || typeof key !== 'object' || !key.hashkey) {
        throw new Error('Invalid pagination cursor');
    }

    return key;
};
