'use strict';

const dynamo = require('../../../shared/lib/dynamo');
const response = require('../../../shared/lib/response');

const DYNAMO_TABLE_BOOKS = process.env.DYNAMO_TABLE_BOOKS || 'books';

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 100;

const encodeCursor = lastEvaluatedKey => {
    if (!lastEvaluatedKey) {
        return null;
    }
    return Buffer.from(JSON.stringify(lastEvaluatedKey)).toString('base64');
};

const decodeCursor = cursor => JSON.parse(Buffer.from(cursor, 'base64').toString('utf8'));

const parseLimit = value => {

    if (value === undefined || value === null || value === '') {
        return { value: DEFAULT_LIMIT };
    }

    const parsed = Number(value);

    if (!Number.isInteger(parsed) || parsed < 1) {
        return { error: 'Query parameter "limit" must be a positive integer' };
    }

    return { value: Math.min(parsed, MAX_LIMIT) };
};

module.exports.list = (event, context, callback) => {

    const query = event.queryStringParameters || {};

    const limit = parseLimit(query.limit);

    if (limit.error) {
        return callback(null, {
            statusCode: 400,
            headers: {
                "Access-Control-Allow-Origin": "*"
            },
            body: JSON.stringify({
                status: 400,
                message: limit.error
            })
        });
    }

    const params = {};

    if (query.cursor) {
        try {
            params.ExclusiveStartKey = decodeCursor(query.cursor);
        } catch (error) {
            return callback(null, {
                statusCode: 400,
                headers: {
                    "Access-Control-Allow-Origin": "*"
                },
                body: JSON.stringify({
                    status: 400,
                    message: 'Invalid pagination cursor'
                })
            });
        }
    }

    dynamo.scan(params, limit.value, DYNAMO_TABLE_BOOKS)
        .then(books => {

            response.json(callback, {
                items: books.Items,
                next_cursor: encodeCursor(books.LastEvaluatedKey)
            });

        }).catch(err => {

            response.json(callback, err, 500);

        });

};

module.exports.detail = (event, context, callback) => {

    const key = {
        hashkey: event.pathParameters.hashkey
    };

    dynamo.find(key, DYNAMO_TABLE_BOOKS)
        .then(book => {

            if (!book.Item) {
                return callback(null, {
                    statusCode: 404,
                    body: JSON.stringify({
                        status: 404,
                        message: "Not Found"
                    })
                });
            }

            callback(null, {
                statusCode: 200,
                body: JSON.stringify(book.Item)
            });

        }).catch(err => {

            response.json(callback, err, 500);

        });

};

module.exports.envs = (event, context, callback) => {

    response.json(callback, {
        env: process.env.ENV || null,
        message: process.env.MESSAGE || null,
        region: process.env.REGION || null
    });

};
