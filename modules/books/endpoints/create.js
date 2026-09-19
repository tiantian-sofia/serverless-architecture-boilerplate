'use strict';

const uuid = require('../../../shared/lib/uuid');
const dynamo = require('../../../shared/lib/dynamo');
const response = require('../../../shared/lib/response');
const sqs = require('../../../shared/lib/sqs');
const logger = require('../../../shared/lib/logger');

const DYNAMO_TABLE_BOOKS = process.env.DYNAMO_TABLE_BOOKS || 'books';
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || 'book';

/**
 * Register a single Book on SQS
 * @param  {[type]}   event    [Event Trigger]
 * @param  {[type]}   context  [Event Context]
 * @param  {Function} callback [Callback to resolve]
 * @return {[type]}            [None]
 *
 * This endpoint receibe a simple POST Payload like this:
 *
 * {
 * 		"title" : "American Gods"
 *      "author" : "Neil Gaiman",
 *      "price" : 10.00
 * }
 *
 * After receibe a simple payload:
 *
 * Register on SQS Queue -> books-consumer will process the event envelope
 * and update the Book on DynamoDB, with idempotency keyed on eventId and
 * optimistic concurrency keyed on version.
 */
module.exports.create = (event, context, callback) => {

    const body = event.body ? event.body : event;
    const data = JSON.parse(body);

    const hashkey = uuid();
    const eventId = uuid();
    const version = Date.now();

    const book = {
        hashkey: hashkey,
        title: data.title,
        author: data.author,
        price: data.price,
        updated_by_worker: 0,
        version: version,
        created: new Date().getTime()
    };

    // Event envelope consumed by the SQS-driven books-consumer.
    const queueEvent = {
        eventId: eventId,
        hashKey: hashkey,
        version: version,
        type: 'book.processed',
        payload: {}
    };

    logger.info('publishing_book_event', {
        eventId: eventId,
        hashKey: hashkey,
        version: version
    });

    /**
     * Save item on DynamoDB and publish the processing event on SQS.
     */
    Promise.all([
            dynamo.save(book, DYNAMO_TABLE_BOOKS),
            sqs.sendToQueue(queueEvent, SQS_QUEUE_URL)
        ])
        .then(success => {
            response.json(callback, success, 201);
        })
        .catch(err => {
            response.json(callback, err, 500);
        });

};
