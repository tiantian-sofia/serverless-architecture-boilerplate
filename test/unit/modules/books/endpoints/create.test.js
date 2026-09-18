'use strict';

const expect = require("chai").expect;

const dynamo = require('../../../../../shared/lib/dynamo');
const sqs = require('../../../../../shared/lib/sqs');
const createBook = require('../../../../../modules/books/endpoints/create').create;

const call = event => new Promise((resolve, reject) => {
    createBook(event, {}, (err, result) => {
        if (err) {
            return reject(err);
        }
        resolve(result);
    });
});

describe("#books create endpoint", () => {

    let originals;
    let saveCalls;
    let sendCalls;

    beforeEach(() => {
        originals = {
            save: dynamo.save,
            sendToQueue: sqs.sendToQueue
        };
        saveCalls = [];
        sendCalls = [];
    });

    afterEach(() => {
        dynamo.save = originals.save;
        sqs.sendToQueue = originals.sendToQueue;
    });

    it("#returns 400 without writing anything when the body is invalid JSON", async () => {
        dynamo.save = () => {
            throw new Error('save should never be called');
        };
        sqs.sendToQueue = () => {
            throw new Error('sendToQueue should never be called');
        };

        const result = await call({ body: '{not-json' });

        expect(result.statusCode).to.equal(400);
        expect(JSON.parse(result.body).message).to.equal('Invalid JSON body');
    });

    it("#persists the book and enqueues its hashkey with a 201", async () => {
        dynamo.save = (...args) => {
            saveCalls.push(args);
            return Promise.resolve({});
        };
        sqs.sendToQueue = (...args) => {
            sendCalls.push(args);
            return Promise.resolve({});
        };

        const result = await call({
            body: JSON.stringify({ title: 'American Gods', author: 'Neil Gaiman', price: 10 })
        });

        expect(result.statusCode).to.equal(201);
        expect(saveCalls).to.have.lengthOf(1);
        expect(saveCalls[0][0].title).to.equal('American Gods');
        expect(saveCalls[0][0].hashkey).to.be.a('string');
        expect(sendCalls).to.have.lengthOf(1);
        expect(sendCalls[0][0].hashkey).to.equal(saveCalls[0][0].hashkey);
    });

});
