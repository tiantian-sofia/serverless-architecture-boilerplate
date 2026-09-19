'use strict';

class FakeSqs {
    constructor() {
        this.sentMessages = [];
        this.failNext = null;
    }

    failSend(error) {
        this.failNext = error;
    }

    sendRaw(params) {
        if (this.failNext) {
            const error = this.failNext;
            this.failNext = null;
            return Promise.reject(error);
        }

        this.sentMessages.push(params);
        return Promise.resolve({ MessageId: `dlq-${this.sentMessages.length}` });
    }
}

module.exports = { FakeSqs };
