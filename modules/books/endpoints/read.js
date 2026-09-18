'use strict';

const dynamo = require('../../../shared/lib/dynamo');
const response = require('../../../shared/lib/response');
const cursor = require('../../../shared/lib/cursor');

const DYNAMO_TABLE_BOOKS = process.env.DYNAMO_TABLE_BOOKS || 'books';

module.exports.list = (event, context, callback) => {

    const query = event.queryStringParameters || {};

    const params = {};
    let limit = null;

    if (query.limit !== undefined && query.limit !== null && query.limit !== '') {
        limit = parseInt(query.limit, 10);

        if (!Number.isInteger(limit) || limit <= 0) {
            return response.json(callback, {
                status: 400,
                message: 'Query parameter "limit" must be a positive integer'
            }, 400);
        }
    }

    if (query.cursor) {
        try {
            params.ExclusiveStartKey = cursor.decode(query.cursor);
        } catch (err) {
            return response.json(callback, {
                status: 400,
                message: 'Invalid pagination cursor'
            }, 400);
        }
    }

    dynamo.scan(params, limit, DYNAMO_TABLE_BOOKS)
        .then(result => {

            response.json(callback, {
                items: result.Items,
                next_cursor: result.LastEvaluatedKey ? cursor.encode(result.LastEvaluatedKey) : null
            });

        }).catch(err => {

            response.json(callback, err, 500);

        })

};

module.exports.detail = (event, context, callback) => {

    dynamo.find({ hashkey: event.pathParameters.hashkey }, DYNAMO_TABLE_BOOKS)
        .then(book => {

            if (!book.Item) {
                return response.json(callback, {
                    status: 404,
                    message: "Not Found"
                }, 404);
            } else {
                return response.json(callback, book.Item);
            }
        }).catch(err => {

            response.json(callback, err, 500);

        });

};

module.exports.envs = (event, context, callback) => {

    response.json(callback, {
        env: process.env.ENV,
        message: process.env.MESSAGE,
        region: process.env.REGION,
        dynamo_table_books: process.env.DYNAMO_TABLE_BOOKS,
        sqs_queue_url: process.env.SQS_QUEUE_URL
    });

};
