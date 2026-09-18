'use strict';

const dynamo = require('../../../shared/lib/dynamo');
const response = require('../../../shared/lib/response');

const DYNAMO_TABLE_BOOKS = process.env.DYNAMO_TABLE_BOOKS || 'books';

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
    data = JSON.parse(body);
  } catch (err) {
    return response.json(callback, {
      status: 400,
      message: 'Invalid JSON body'
    }, 400);
  }

  const key = {
    hashkey: event.pathParameters.hashkey
  };

  const attributes = {};

  if (data.title !== undefined) {
    attributes.title = {
      Action: 'PUT',
      Value: data.title
    };
  }

  if (data.author !== undefined) {
    attributes.author = {
      Action: 'PUT',
      Value: data.author
    };
  }

  if (data.price !== undefined) {
    attributes.price = {
      Action: 'PUT',
      Value: data.price
    };
  }

  if (Object.keys(attributes).length === 0) {
    return response.json(callback, {
      status: 400,
      message: 'At least one of the attributes "title", "author" or "price" must be provided'
    }, 400);
  }

  dynamo.find(key, DYNAMO_TABLE_BOOKS)
    .then(existing => {

      if (!existing.Item) {
        return response.json(callback, {
          status: 404,
          message: 'Not Found'
        }, 404);
      }

      return dynamo.updateItem(key, attributes, DYNAMO_TABLE_BOOKS).then(success => {

        response.json(callback, success.Attributes);

      });

    })
    .catch(err => {

      response.json(callback, err, 500);

    });

};
