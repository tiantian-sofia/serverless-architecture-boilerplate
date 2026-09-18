'use strict';

const expect = require("chai").expect;

const cursor = require('../../../../shared/lib/cursor');

describe("#cursor library", () => {

    it("#Have encode() method", () => {
        expect(cursor.encode).to.be.an('function');
    });

    it("#Have decode() method", () => {
        expect(cursor.decode).to.be.an('function');
    });

    it("#Encode a DynamoDB key and decode it back", () => {
        const key = { hashkey: 'abc-123' };

        const encoded = cursor.encode(key);

        expect(encoded).to.be.a('string');
        expect(encoded).to.not.contain('+');
        expect(encoded).to.not.contain('/');
        expect(encoded).to.not.contain('=');
        expect(cursor.decode(encoded)).to.deep.equal(key);
    });

    it("#Reject a malformed cursor", () => {
        expect(() => cursor.decode('not-base64-json!!')).to.throw();
    });

    it("#Reject a cursor that does not contain a hashkey", () => {
        const encoded = cursor.encode ? Buffer.from(JSON.stringify({ foo: 'bar' })).toString('base64') : '';

        expect(() => cursor.decode(encoded)).to.throw('Invalid pagination cursor');
    });

});
