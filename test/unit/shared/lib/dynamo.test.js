const expect = require("chai").expect;

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

});

describe("#dynamodb library params", () => {

    const original = {};
    let captured;

    beforeEach(() => {
        captured = {};
        original.scan = AWS.DynamoDB.DocumentClient.prototype.scan;
        original.get = AWS.DynamoDB.DocumentClient.prototype.get;
        AWS.DynamoDB.DocumentClient.prototype.scan = function(params) {
            captured.scan = params;
            return { promise: () => Promise.resolve({ Items: [] }) };
        };
        AWS.DynamoDB.DocumentClient.prototype.get = function(params) {
            captured.get = params;
            return { promise: () => Promise.resolve({}) };
        };
    });

    afterEach(() => {
        AWS.DynamoDB.DocumentClient.prototype.scan = original.scan;
        AWS.DynamoDB.DocumentClient.prototype.get = original.get;
    });

    it("#scan applies the limit argument to the DynamoDB request", async() => {
        await dynamo.scan({}, 25, 'my-table');
        expect(captured.scan.TableName).to.equal('my-table');
        expect(captured.scan.Limit).to.equal(25);
    });

    it("#scan forwards pagination options and omits Limit when limit is null", async() => {
        await dynamo.scan({ ExclusiveStartKey: { hashkey: 'abc' } }, null, 'my-table');
        expect(captured.scan.TableName).to.equal('my-table');
        expect(captured.scan.ExclusiveStartKey).to.deep.equal({ hashkey: 'abc' });
        expect(captured.scan).to.not.have.property('Limit');
    });

    it("#scan works without params argument", async() => {
        await dynamo.scan(undefined, 10, 'my-table');
        expect(captured.scan.TableName).to.equal('my-table');
        expect(captured.scan.Limit).to.equal(10);
    });

    it("#find uses the table argument instead of the global table", async() => {
        await dynamo.find({ hashkey: 'abc' }, 'my-table');
        expect(captured.get.TableName).to.equal('my-table');
        expect(captured.get.Key).to.deep.equal({ hashkey: 'abc' });
    });

});
