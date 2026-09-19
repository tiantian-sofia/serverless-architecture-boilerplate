'use strict';

const AWS = require('aws-sdk');

/**
 * SQS client factory.
 *
 * The consumer is driven by the SQS Event Source Mapping: Lambda polls
 * the queue on our behalf. Application code therefore MUST NOT call
 * ReceiveMessage or DeleteMessage - deletion is performed by the Event
 * Source Mapping according to the ReportBatchItemFailures response.
 *
 * This library is intentionally publish-only (SendMessage), used by the
 * HTTP producer and by the consumer to quarantine poison messages in the
 * DLQ.
 */

const LOCAL_ENDPOINT = 'http://sqs:9324';

function buildOptions() {
    if (process.env.IS_OFFLINE) {
        return {
            apiVersion: '2012-11-05',
            region: process.env.REGION || 'localhost',
            endpoint: LOCAL_ENDPOINT,
            sslEnabled: false,
            accessKeyId: 'MOCK_ACCESS_KEY_ID',
            secretAccessKey: 'MOCK_SECRET_ACCESS_KEY'
        };
    }

    return {
        apiVersion: '2012-11-05',
        region: process.env.REGION || 'us-east-1'
    };
}

function createClient(options) {
    const sqs = new AWS.SQS(options || buildOptions());

    function resolveUrl(queue) {
        if (!queue) {
            throw new Error('SQS queue url is required');
        }
        return process.env.IS_OFFLINE ? LOCAL_ENDPOINT + '/queue/' + queue : queue;
    }

    return {
        raw: sqs,

        /**
         * Send a message to a queue. `message` is JSON-encoded when it is
         * not already a string. SQS MessageAttributes are passed through.
         *
         * @param {*} message
         * @param {String} queue
         * @param {Object} [attributes] SQS MessageAttributes
         */
        sendToQueue: (message, queue, attributes) => {
            const params = {
                QueueUrl: resolveUrl(queue),
                MessageBody: typeof message === 'string' ? message : JSON.stringify(message)
            };

            if (attributes && Object.keys(attributes).length > 0) {
                params.MessageAttributes = attributes;
            }

            return sqs.sendMessage(params).promise();
        }
    };
}

const client = createClient();
client.createClient = createClient;

// Backwards compatible alias for existing producers.
client.save = client.sendToQueue;

module.exports = client;
