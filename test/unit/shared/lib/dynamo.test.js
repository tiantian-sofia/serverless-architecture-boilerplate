const expect = require("chai").expect;
const assert = require("chai").assert;

const AWS = require('aws-sdk');
const dynamo = require('../../../../shared/lib/dynamo');

describe("#dynamodb library signature", () => {

    it("#Have save() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('save');
        expect(dynamo.save).to.be.an('function');
    });

    it("#Have find() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('find');
        expect(dynamo.find).to.be.an('function');
    });

    it("#Have query() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('query');
        expect(dynamo.query).to.be.an('function');
    });

    it("#Have scan() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('scan');
        expect(dynamo.scan).to.be.an('function');
    });

    it("#Have update() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('update');
        expect(dynamo.update).to.be.an('function');
    });

    it("#Have updateItem() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('updateItem');
        expect(dynamo.updateItem).to.be.an('function');
    });

    it("#Have removeRow() method", () => {
        expect(dynamo).to.be.an('object').and.include.all.keys('removeRow');
        expect(dynamo.removeRow).to.be.an('function');
    });

});

describe("#dynamodb library behavior", () => {

    const client = AWS.DynamoDB.DocumentClient.prototype;
    let originals;

    beforeEach(() => {
        originals = {};
    });

    afterEach(() => {
        Object.keys(originals).forEach(method => {
            client[method] = originals[method];
        });
    });

    const stubMethod = (method, response) => {
        originals[method] = client[method];
        client[method] = params => {
            client[method].lastParams = params;
            return {
                promise: () => Promise.resolve(response)
            };
        };
        return client[method];
    };

    it("#scan() forwards the limit argument as Limit", async () => {
        const stub = stubMethod('scan', { Items: [] });

        await dynamo.scan({}, 10, 'my-table');

        expect(stub.lastParams.TableName).to.equal('my-table');
        expect(stub.lastParams.Limit).to.equal(10);
    });

    it("#scan() does not set Limit when no limit is informed", async () => {
        const stub = stubMethod('scan', { Items: [] });

        await dynamo.scan({}, null, 'my-table');

        expect(stub.lastParams.TableName).to.equal('my-table');
        expect(stub.lastParams).to.not.have.property('Limit');
    });

    it("#find() reads by key on the informed table", async () => {
        const stub = stubMethod('get', { Item: { hashkey: '123' } });

        const result = await dynamo.find({ hashkey: '123' }, 'my-table');

        expect(stub.lastParams.TableName).to.equal('my-table');
        expect(stub.lastParams.Key).to.deep.equal({ hashkey: '123' });
        expect(result.Item.hashkey).to.equal('123');
    });

    it("#query() uses the informed table", async () => {
        const stub = stubMethod('query', { Items: [] });

        await dynamo.query({ KeyConditionExpression: 'hashkey = :hashkey' }, 'my-table');

        expect(stub.lastParams.TableName).to.equal('my-table');
        expect(stub.lastParams.KeyConditionExpression).to.equal('hashkey = :hashkey');
    });

});
