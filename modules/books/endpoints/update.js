'use strict';

const dynamo = require('../../../shared/lib/dynamo');
const response = require('../../../shared/lib/response');

const DYNAMO_TABLE_BOOKS = process.env.DYNAMO_TABLE_BOOKS || 'books';

const assignAttribute = (params, name, value) => {
    if (value !== undefined && value !== null) {
        params[name] = {
            Action: 'PUT',
            Value: value
        };
    }
};

const notFound = callback => {
    return callback(null, {
        statusCode: 404,
        headers: {
            "Access-Control-Allow-Origin": "*"
        },
        body: JSON.stringify({
            status: 404,
            message: "Not Found"
        })
    });
};

/**
 * Update Item with PUT request
 *
 * {
 *  "title" : "updated"
 * }
 *
 * @param {*} event
 * @param {*} context
 * @param {*} callback
 */
module.exports.update = (event, context, callback) => {

    let data;

    try {
        const body = event.body ? event.body : event;
        data = typeof body === 'string' ? JSON.parse(body) : body;
    } catch (error) {
        return response.json(callback, {
            status: 400,
            message: 'Invalid JSON body'
        }, 400);
    }

    const key = {
        hashkey: event.pathParameters.hashkey
    };

    dynamo.find(key, DYNAMO_TABLE_BOOKS).then(book => {

        if (!book.Item) {
            return notFound(callback);
        }

        const params = {};

        assignAttribute(params, 'title', data.title);
        assignAttribute(params, 'author', data.author);
        assignAttribute(params, 'price', data.price);

        if (Object.keys(params).length === 0) {
            return response.json(callback, book.Item);
        }

        return dynamo.updateItem(key, params, DYNAMO_TABLE_BOOKS, {
            ConditionExpression: 'attribute_exists(hashkey)'
        }).then(success => {

            response.json(callback, success.Attributes);

        }).catch(err => {

            if (err && err.code === 'ConditionalCheckFailedException') {
                return notFound(callback);
            }

            response.json(callback, err, 500);
        });

    }).catch(err => {

        response.json(callback, err, 500);

    });

};
