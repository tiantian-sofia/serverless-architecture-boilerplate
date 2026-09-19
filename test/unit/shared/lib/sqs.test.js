const expect = require("chai").expect;

const sqs = require('../../../../shared/lib/sqs');

describe("#sqs publish-only library signature", () => {

    it("#Have sendToQueue() method", () => {
        expect(sqs.sendToQueue).to.be.an('function');
    });

    it("#Have save() backwards compatible alias", () => {
        expect(sqs.save).to.equal(sqs.sendToQueue);
    });

    it("#Expose a createClient() factory", () => {
        expect(sqs.createClient).to.be.an('function');
        const custom = sqs.createClient({ region: 'us-east-1' });
        expect(custom.sendToQueue).to.be.an('function');
        expect(custom.raw).to.be.an('object');
    });

    it("#No longer expose polling or deletion helpers", () => {
        // The Event Source Mapping owns ReceiveMessage/DeleteMessage.
        expect(sqs.consumeQueue).to.equal(undefined);
        expect(sqs.removeFromQueue).to.equal(undefined);
    });

});
