const expect = require("chai").expect;

const sqs = require('../../../../shared/lib/sqs');

describe("#sqs library signature", () => {
    it("#Have save() method", () => {
        expect(sqs).to.be.an('object').and.to.include.all.keys('save');
        expect(sqs.save).to.be.an('function');
    });

    it("#Have sendToQueue() method", () => {
        expect(sqs).to.be.an('object').and.to.include.all.keys('sendToQueue');
        expect(sqs.sendToQueue).to.be.an('function');
    });

    it("#Have sendRaw() method", () => {
        expect(sqs).to.be.an('object').and.to.include.all.keys('sendRaw');
        expect(sqs.sendRaw).to.be.an('function');
    });

    it("#Does not actively poll SQS", () => {
        expect(sqs.consumeQueue).to.equal(undefined);
        expect(sqs.removeFromQueue).to.equal(undefined);
    });
});
