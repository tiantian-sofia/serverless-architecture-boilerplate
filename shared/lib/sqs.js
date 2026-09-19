'use strict';

const AWS = require("aws-sdk");
AWS.config.setPromisesDependency(require('bluebird'));

const endpoint = process.env.SQS_QUEUE_URL;

const local = "http://sqs:9324";

const dev = {
    apiVersion: '2012-11-05',
    region: process.env.REGION || 'localhost',
    endpoint: local,
    sslEnabled: false,
    accessKeyId: 'MOCK_ACCESS_KEY_ID',
    secretAccessKey: 'MOCK_SECRET_ACCESS_KEY',
};

const prod = {
    apiVersion: '2012-11-05', 
    region: process.env.REGION || 'sa-east-1'
}

const options  = process.env.IS_OFFLINE ? dev : prod

const _sqs = new AWS.SQS(options);

/**
 * SQS Abstraction Library
 * @Author: Matheus 'Raj' Fidelis <msfidelis01@gmail.com>
 * @save() - Interface Method. - Save Item on SQS Queue;
 * @sendToQueue() - Save Item on SQS Queue;
 * @sendRaw() - Send an already built SQS message;
 */
const client = {

    /**
     * Save new message into queue
     */
    save: (message, queue=endpoint) => {

        const url = process.env.IS_OFFLINE ? `${local}/queue/${queue}` : queue;

        const params = {
            QueueUrl: url,
            MessageBody: JSON.stringify(message)
        };

        return _sqs.sendMessage(params).promise();
    },
    /**
     * Send message to queue
     */
    sendToQueue: (message, queue=endpoint) => {

        const url = process.env.IS_OFFLINE  ? `${local}/queue/${queue}` : queue;

        const params = {
            QueueUrl: url,
            MessageBody: JSON.stringify(message)
        };

        return _sqs.sendMessage(params).promise();
    },
    /**
     * Send a raw SQS message
     */
    sendRaw: params => {
        return _sqs.sendMessage(params).promise();
    }
}

module.exports = client;
