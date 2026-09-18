'use strict';

const expect = require("chai").expect;

const dynamo = require('../../../../../shared/lib/dynamo');
const updateBook = require('../../../../../modules/books/endpoints/update').update;

const TABLE = process.env.DYNAMO_TABLE_BOOKS || 'books';

const call = event => new Promise((resolve, reject) => {
    updateBook(event, {}, (err, result) => {
        if (err) {
            return reject(err);
        }
        resolve(result);
    });
});

describe("#books update endpoint", () => {

    let originals;
    let findCalls;
    let updateCalls;

    beforeEach(() => {
        originals = {
            find: dynamo.find,
            updateItem: dynamo.updateItem
        };
        findCalls = [];
        updateCalls = [];
    });

    afterEach(() => {
        dynamo.find = originals.find;
        dynamo.updateItem = originals.updateItem;
    });

    const setFind = behavior => {
        dynamo.find = (...args) => {
            findCalls.push(args);
            return behavior();
        };
    };

    const setUpdate = behavior => {
        dynamo.updateItem = (...args) => {
            updateCalls.push(args);
            return behavior();
        };
    };

    const eventFor = (hashkey, payload) => ({
        pathParameters: { hashkey },
        body: JSON.stringify(payload)
    });

    it("#returns 404 and performs no writes when the item does not exist", async () => {
        setFind(() => Promise.resolve({}));
        setUpdate(() => {
            throw new Error('updateItem should never be called');
        });

        const result = await call(eventFor('missing', { title: 'updated' }));

        expect(result.statusCode).to.equal(404);
        expect(JSON.parse(result.body)).to.deep.equal({
            status: 404,
            message: 'Not Found'
        });
        expect(findCalls[0][0]).to.deep.equal({ hashkey: 'missing' });
        expect(updateCalls).to.have.lengthOf(0);
    });

    it("#updates an existing item and returns the new attributes", async () => {
        setFind(() => Promise.resolve({
            Item: { hashkey: '42', title: 'old' }
        }));
        setUpdate(() => Promise.resolve({
            Attributes: { title: 'new' }
        }));

        const result = await call(eventFor('42', { title: 'new' }));

        expect(result.statusCode).to.equal(200);
        expect(JSON.parse(result.body)).to.deep.equal({ title: 'new' });
        expect(updateCalls[0][0]).to.deep.equal({ hashkey: '42' });
        expect(updateCalls[0][1]).to.deep.equal({
            title: { Action: 'PUT', Value: 'new' }
        });
        expect(updateCalls[0][2]).to.equal(TABLE);
    });

    it("#allows updating the price to zero", async () => {
        setFind(() => Promise.resolve({ Item: { hashkey: '42' } }));
        setUpdate(() => Promise.resolve({ Attributes: { price: 0 } }));

        const result = await call(eventFor('42', { price: 0 }));

        expect(result.statusCode).to.equal(200);
        expect(updateCalls[0][1]).to.deep.equal({
            price: { Action: 'PUT', Value: 0 }
        });
    });

    it("#allows clearing a field with an empty string", async () => {
        setFind(() => Promise.resolve({ Item: { hashkey: '42' } }));
        setUpdate(() => Promise.resolve({ Attributes: { author: '' } }));

        const result = await call(eventFor('42', { author: '' }));

        expect(result.statusCode).to.equal(200);
        expect(updateCalls[0][1]).to.deep.equal({
            author: { Action: 'PUT', Value: '' }
        });
    });

    it("#returns 400 when the body is not valid JSON", async () => {
        const result = await call({
            pathParameters: { hashkey: '42' },
            body: '{not-json'
        });

        expect(result.statusCode).to.equal(400);
        expect(findCalls).to.have.lengthOf(0);
        expect(updateCalls).to.have.lengthOf(0);
    });

    it("#returns 400 when no updatable attribute is sent", async () => {
        const result = await call(eventFor('42', { unknown: 'field' }));

        expect(result.statusCode).to.equal(400);
        expect(findCalls).to.have.lengthOf(0);
        expect(updateCalls).to.have.lengthOf(0);
    });

    it("#returns 500 when the existence check fails", async () => {
        setFind(() => Promise.reject(new Error('dynamo down')));

        const result = await call(eventFor('42', { title: 'new' }));

        expect(result.statusCode).to.equal(500);
        expect(updateCalls).to.have.lengthOf(0);
    });

    it("#returns 500 when the write itself fails", async () => {
        setFind(() => Promise.resolve({ Item: { hashkey: '42' } }));
        setUpdate(() => Promise.reject(new Error('dynamo down')));

        const result = await call(eventFor('42', { title: 'new' }));

        expect(result.statusCode).to.equal(500);
    });

});
