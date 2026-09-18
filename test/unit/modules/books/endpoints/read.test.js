const expect = require("chai").expect;

const AWS = require('aws-sdk');
const read = require('../../../../../modules/books/endpoints/read');

const flush = () => new Promise(resolve => setImmediate(() => setImmediate(resolve)));

describe("#books list endpoint", () => {

    const original = {};
    let scanStub;

    beforeEach(() => {
        original.scan = AWS.DynamoDB.DocumentClient.prototype.scan;
        scanStub = AWS.DynamoDB.DocumentClient.prototype.scan = function(params) {
            return { promise: () => scanStub.result(params) };
        };
    });

    afterEach(() => {
        AWS.DynamoDB.DocumentClient.prototype.scan = original.scan;
    });

    const invokeList = (queryStringParameters) => {
        const calls = [];
        read.list({ queryStringParameters }, {}, (err, response) => calls.push({ err, response }));
        return calls;
    };

    it("#Returns items and null next_cursor when there is no more data", async() => {
        scanStub.result = () => Promise.resolve({ Items: [{ hashkey: 'book-1' }] });

        const calls = invokeList({});
        await flush();

        expect(calls).to.have.lengthOf(1);
        expect(calls[0].response.statusCode).to.equal(200);
        const body = JSON.parse(calls[0].response.body);
        expect(body.items).to.deep.equal([{ hashkey: 'book-1' }]);
        expect(body.next_cursor).to.equal(null);
    });

    it("#Uses a default limit when none is provided", async() => {
        let sentParams;
        scanStub.result = (params) => {
            sentParams = params;
            return Promise.resolve({ Items: [] });
        };

        invokeList({});
        await flush();

        expect(sentParams.Limit).to.equal(100);
        expect(sentParams).to.not.have.property('ExclusiveStartKey');
    });

    it("#Forwards the limit query parameter to DynamoDB", async() => {
        let sentParams;
        scanStub.result = (params) => {
            sentParams = params;
            return Promise.resolve({ Items: [] });
        };

        invokeList({ limit: '10' });
        await flush();

        expect(sentParams.Limit).to.equal(10);
    });

    it("#Caps the limit to the maximum allowed value", async() => {
        let sentParams;
        scanStub.result = (params) => {
            sentParams = params;
            return Promise.resolve({ Items: [] });
        };

        invokeList({ limit: '9999' });
        await flush();

        expect(sentParams.Limit).to.equal(100);
    });

    it("#Rejects non-positive and non-integer limits with 400", async() => {
        const calls = invokeList({ limit: '0' });
        await flush();

        expect(calls).to.have.lengthOf(1);
        expect(calls[0].response.statusCode).to.equal(400);
    });

    it("#Returns a next_cursor built from LastEvaluatedKey", async() => {
        scanStub.result = () => Promise.resolve({
            Items: [{ hashkey: 'book-1' }],
            LastEvaluatedKey: { hashkey: 'book-1' }
        });

        const calls = invokeList({ limit: '1' });
        await flush();

        const body = JSON.parse(calls[0].response.body);
        const decoded = JSON.parse(Buffer.from(body.next_cursor, 'base64').toString('utf8'));
        expect(decoded).to.deep.equal({ hashkey: 'book-1' });
    });

    it("#Decodes the cursor query parameter into ExclusiveStartKey", async() => {
        const cursor = Buffer.from(JSON.stringify({ hashkey: 'book-1' })).toString('base64');
        let sentParams;
        scanStub.result = (params) => {
            sentParams = params;
            return Promise.resolve({ Items: [] });
        };

        invokeList({ cursor });
        await flush();

        expect(sentParams.ExclusiveStartKey).to.deep.equal({ hashkey: 'book-1' });
    });

    it("#Rejects malformed cursors with 400", async() => {
        const calls = invokeList({ cursor: 'not-a-valid-cursor' });
        await flush();

        expect(calls[0].response.statusCode).to.equal(400);
        expect(JSON.parse(calls[0].response.body).message).to.equal('Invalid pagination cursor');
    });

    it("#Returns 500 when DynamoDB fails", async() => {
        scanStub.result = () => Promise.reject(new Error('dynamo down'));

        const calls = invokeList({});
        await flush();

        expect(calls[0].response.statusCode).to.equal(500);
    });

});

describe("#books detail endpoint", () => {

    const original = {};
    let getStub;

    beforeEach(() => {
        original.get = AWS.DynamoDB.DocumentClient.prototype.get;
        getStub = AWS.DynamoDB.DocumentClient.prototype.get = function(params) {
            return { promise: () => getStub.result(params) };
        };
    });

    afterEach(() => {
        AWS.DynamoDB.DocumentClient.prototype.get = original.get;
    });

    const invokeDetail = (hashkey) => {
        const calls = [];
        read.detail({ pathParameters: { hashkey } }, {}, (err, response) => calls.push({ err, response }));
        return calls;
    };

    it("#Reads directly by primary key", async() => {
        let sentParams;
        getStub.result = (params) => {
            sentParams = params;
            return Promise.resolve({ Item: { hashkey: 'book-1', title: 'American Gods' } });
        };

        const calls = invokeDetail('book-1');
        await flush();

        expect(sentParams.Key).to.deep.equal({ hashkey: 'book-1' });
        expect(calls[0].response.statusCode).to.equal(200);
        expect(JSON.parse(calls[0].response.body).title).to.equal('American Gods');
    });

    it("#Keeps the original 404 payload when the item does not exist", async() => {
        getStub.result = () => Promise.resolve({});

        const calls = invokeDetail('missing');
        await flush();

        expect(calls[0].response.statusCode).to.equal(404);
        expect(JSON.parse(calls[0].response.body)).to.deep.equal({
            status: 404,
            message: "Not Found"
        });
    });

    it("#Returns 500 when DynamoDB fails", async() => {
        getStub.result = () => Promise.reject(new Error('dynamo down'));

        const calls = invokeDetail('book-1');
        await flush();

        expect(calls[0].response.statusCode).to.equal(500);
    });

});

describe("#books envs endpoint", () => {

    it("#Exposes runtime environment information", () => {
        const calls = [];
        read.envs({}, {}, (err, response) => calls.push({ err, response }));

        expect(calls).to.have.lengthOf(1);
        expect(calls[0].response.statusCode).to.equal(200);
        const body = JSON.parse(calls[0].response.body);
        expect(body).to.have.all.keys('env', 'message', 'region');
    });

});
