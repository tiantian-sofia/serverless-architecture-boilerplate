const expect = require("chai").expect;

const AWS = require('aws-sdk');
const updateEndpoint = require('../../../../../modules/books/endpoints/update');

const flush = () => new Promise(resolve => setImmediate(() => setImmediate(resolve)));

describe("#books update endpoint", () => {

    const original = {};
    let getStub;
    let updateStub;
    let getResult;
    let capturedUpdate;

    beforeEach(() => {
        original.get = AWS.DynamoDB.DocumentClient.prototype.get;
        original.update = AWS.DynamoDB.DocumentClient.prototype.update;

        capturedUpdate = [];
        getResult = { Item: { hashkey: 'book-1', title: 'old' } };

        getStub = AWS.DynamoDB.DocumentClient.prototype.get = function() {
            return { promise: () => Promise.resolve(getResult) };
        };
        updateStub = AWS.DynamoDB.DocumentClient.prototype.update = function(params) {
            capturedUpdate.push(params);
            return { promise: () => Promise.resolve({ Attributes: Object.assign({}, getResult.Item, { title: 'new' }) }) };
        };
    });

    afterEach(() => {
        AWS.DynamoDB.DocumentClient.prototype.get = original.get;
        AWS.DynamoDB.DocumentClient.prototype.update = original.update;
    });

    const invokeUpdate = (hashkey, body) => {
        const calls = [];
        const event = { pathParameters: { hashkey }, body: typeof body === 'string' ? body : JSON.stringify(body || {}) };
        updateEndpoint.update(event, {}, (err, response) => calls.push({ err, response }));
        return calls;
    };

    it("#Returns 404 and never writes when the item does not exist", async() => {
        getResult = {};

        const calls = invokeUpdate('missing', { title: 'new' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(404);
        expect(JSON.parse(calls[0].response.body)).to.deep.equal({
            status: 404,
            message: "Not Found"
        });
        expect(capturedUpdate).to.have.lengthOf(0);
    });

    it("#Updates an existing item and returns the new attributes", async() => {
        const calls = invokeUpdate('book-1', { title: 'new' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(200);
        expect(JSON.parse(calls[0].response.body).title).to.equal('new');
        expect(capturedUpdate).to.have.lengthOf(1);
        expect(capturedUpdate[0].Key).to.deep.equal({ hashkey: 'book-1' });
        expect(capturedUpdate[0].AttributeUpdates.title.Value).to.equal('new');
        expect(capturedUpdate[0].ConditionExpression).to.equal('attribute_exists(hashkey)');
    });

    it("#Returns 404 when the item is deleted between the read and the write", async() => {
        const conditionalError = Object.assign(new Error('conditional check failed'), {
            code: 'ConditionalCheckFailedException'
        });
        AWS.DynamoDB.DocumentClient.prototype.update = function() {
            return { promise: () => Promise.reject(conditionalError) };
        };

        const calls = invokeUpdate('book-1', { title: 'new' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(404);
        expect(JSON.parse(calls[0].response.body).message).to.equal("Not Found");
    });

    it("#Allows updating numeric fields to zero", async() => {
        invokeUpdate('book-1', { price: 0 });
        await flush();

        expect(capturedUpdate).to.have.lengthOf(1);
        expect(capturedUpdate[0].AttributeUpdates.price.Value).to.equal(0);
    });

    it("#Ignores unknown fields and returns the existing item when nothing is updatable", async() => {
        const calls = invokeUpdate('book-1', { unknown_field: 'value' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(200);
        expect(JSON.parse(calls[0].response.body)).to.deep.equal({ hashkey: 'book-1', title: 'old' });
        expect(capturedUpdate).to.have.lengthOf(0);
    });

    it("#Returns 400 when the body is not valid JSON", async() => {
        const calls = invokeUpdate('book-1', '{not-json');
        await flush();

        expect(calls[0].response.statusCode).to.equal(400);
        expect(capturedUpdate).to.have.lengthOf(0);
    });

    it("#Returns 500 when the existence check fails", async() => {
        AWS.DynamoDB.DocumentClient.prototype.get = function() {
            return { promise: () => Promise.reject(new Error('dynamo down')) };
        };

        const calls = invokeUpdate('book-1', { title: 'new' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(500);
        expect(capturedUpdate).to.have.lengthOf(0);
    });

    it("#Returns 500 when the update itself fails", async() => {
        AWS.DynamoDB.DocumentClient.prototype.update = function() {
            return { promise: () => Promise.reject(new Error('dynamo down')) };
        };

        const calls = invokeUpdate('book-1', { title: 'new' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(500);
    });

});
