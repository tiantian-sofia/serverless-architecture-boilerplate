'use strict';

class PermanentMessageError extends Error {
    constructor(message, details) {
        super(message);
        this.name = 'PermanentMessageError';
        this.details = details || {};
    }
}

class RetryableProcessingError extends Error {
    constructor(message, details) {
        super(message);
        this.name = 'RetryableProcessingError';
        this.details = details || {};
    }
}

class DuplicateEventError extends PermanentMessageError {
    constructor(message, details) {
        super(message, details);
        this.name = 'DuplicateEventError';
    }
}

class StaleEventError extends PermanentMessageError {
    constructor(message, details) {
        super(message, details);
        this.name = 'StaleEventError';
    }
}

module.exports = {
    PermanentMessageError,
    RetryableProcessingError,
    DuplicateEventError,
    StaleEventError
};
