'use strict';

const expect = require("chai").expect;

const dynamo = require('../../../../../shared/lib/dynamo');
const cursor = require('../../../../../shared/lib/cursor');
const read = require('../../../../../modules/books/endpoints/read');

const TABLE = process.env.DYNAMO_TABLE_BOOKS || 'books';

const call = (handler, event) => new Promise((resolve, reject) => {
    handler(event, {}, (err, result) => {
        if (err) {
            return reject(err);
        }
        resolve(result);
    });
});

describe("#books read endpoints", () => {

    let originals;

    beforeEach(() => {
        originals = {};
    });

    afterEach(() => {
        Object.keys(originals).forEach(method => {
            dynamo[method] = originals[method];
        });
    });

    const stub = (method, behavior) => {
        originals[method] = dynamo[method];
        const calls = [];
        dynamo[method] = (...args) => {
            calls.push(args);
            return behavior(args);
        };
        return calls;
    };

    describe("#list", () => {

        it("#returns items and a null cursor when there is no more data", async () => {
            stub('scan', () => Promise.resolve({
                Items: [{ hashkey: '1' }, { hashkey: '2' }]
            }));

            const result = await call(read.list, { queryStringParameters: null });

            expect(result.statusCode).to.equal(200);
            expect(JSON.parse(result.body)).to.deep.equal({
                items: [{ hashkey: '1' }, { hashkey: '2' }],
                next_cursor: null
            });
        });

        it("#returns an encoded next_cursor when the table has more pages", async () => {
            stub('scan', () => Promise.resolve({
                Items: [{ hashkey: '1' }],
                LastEvaluatedKey: { hashkey: '1' }
            }));

            const result = await call(read.list, { queryStringParameters: {} });
            const body = JSON.parse(result.body);

            expect(body.items).to.deep.equal([{ hashkey: '1' }]);
            expect(cursor.decode(body.next_cursor)).to.deep.equal({ hashkey: '1' });
        });

        it("#forwards the limit and table name to the scan client", async () => {
            const calls = stub('scan', () => Promise.resolve({ Items: [] }));

            await call(read.list, { queryStringParameters: { limit: '25' } });

            expect(calls[0][0]).to.deep.equal({});
            expect(calls[0][1]).to.equal(25);
            expect(calls[0][2]).to.equal(TABLE);
        });

        it("#decodes the cursor into ExclusiveStartKey", async () => {
            const calls = stub('scan', () => Promise.resolve({ Items: [] }));

            const nextCursor = cursor.encode({ hashkey: '99' });

            await call(read.list, { queryStringParameters: { cursor: nextCursor } });

            expect(calls[0][0]).to.deep.equal({
                ExclusiveStartKey: { hashkey: '99' }
            });
        });

        it("#rejects a non positive integer limit", async () => {
            stub('scan', () => {
                throw new Error('scan should not be called');
            });

            const result = await call(read.list, { queryStringParameters: { limit: '0' } });

            expect(result.statusCode).to.equal(400);
            expect(JSON.parse(result.body).message).to.contain('limit');
        });

        it("#rejects a malformed cursor with a 400", async () => {
            const result = await call(read.list, { queryStringParameters: { cursor: 'invalid!!!' } });

            expect(result.statusCode).to.equal(400);
        });

        it("#returns 500 when dynamo fails", async () => {
            stub('scan', () => Promise.reject(new Error('dynamo down')));

            const result = await call(read.list, { queryStringParameters: {} });

            expect(result.statusCode).to.equal(500);
        });

    });

    describe("#detail", () => {

        it("#reads directly by partition key instead of scanning", async () => {
            const calls = stub('find', () => Promise.resolve({
                Item: { hashkey: '42', title: 'American Gods' }
            }));

            const result = await call(read.detail, { pathParameters: { hashkey: '42' } });

            expect(calls[0][0]).to.deep.equal({ hashkey: '42' });
            expect(calls[0][1]).to.equal(TABLE);
            expect(result.statusCode).to.equal(200);
            expect(JSON.parse(result.body)).to.deep.equal({ hashkey: '42', title: 'American Gods' });
        });

        it("#keeps the original 404 behavior when the item does not exist", async () => {
            stub('find', () => Promise.resolve({}));

            const result = await call(read.detail, { pathParameters: { hashkey: 'missing' } });

            expect(result.statusCode).to.equal(404);
            expect(JSON.parse(result.body)).to.deep.equal({
                status: 404,
                message: 'Not Found'
            });
        });

        it("#returns 500 when dynamo fails", async () => {
            stub('find', () => Promise.reject(new Error('dynamo down')));

            const result = await call(read.detail, { pathParameters: { hashkey: '42' } });

            expect(result.statusCode).to.equal(500);
        });

    });

    describe("#envs", () => {

        it("#is exported as a function", () => {
            expect(read.envs).to.be.an('function');
        });

        it("#returns runtime configuration values", async () => {
            process.env.DYNAMO_TABLE_BOOKS = 'test-BooksCatalog';
            const result = await call(read.envs, {});
            const body = JSON.parse(result.body);
            delete process.env.DYNAMO_TABLE_BOOKS;

            expect(result.statusCode).to.equal(200);
            expect(body.dynamo_table_books).to.equal('test-BooksCatalog');
        });

    });

});
