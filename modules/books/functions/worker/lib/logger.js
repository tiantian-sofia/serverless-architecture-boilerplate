'use strict';

const DEFAULTS = {
    service: process.env.SERVICE_NAME || 'serverless-boilerplate',
    component: 'books-consumer',
    stage: process.env.ENV || process.env.STAGE || 'unknown'
};

const METRIC_NAMESPACE = process.env.METRICS_NAMESPACE || 'ServerlessBoilerplate/BooksConsumer';

function safeError(err) {
    if (!err) {
        return undefined;
    }

    return {
        name: err.name || 'Error',
        message: err.message,
        code: err.code,
        statusCode: err.statusCode,
        details: err.details
    };
}

function logger(level, message, fields) {
    const payload = Object.assign({
        level,
        message,
        timestamp: new Date().toISOString()
    }, DEFAULTS, fields);

    if (payload.error instanceof Error) {
        payload.error = safeError(payload.error);
    }

    const line = JSON.stringify(payload);

    if (level === 'error') {
        console.error(line);
        return;
    }

    console.info(line);
}

function emitMetric(name, value, dimensions) {
    const dimensionValues = Object.assign({
        component: DEFAULTS.component,
        stage: DEFAULTS.stage
    }, dimensions);
    const dimensionKeys = Object.keys(dimensionValues);

    console.info(JSON.stringify(Object.assign({
        _aws: {
            Timestamp: Date.now(),
            CloudWatchMetrics: [{
                Namespace: METRIC_NAMESPACE,
                Dimensions: [dimensionKeys],
                Metrics: [{
                    Name: name,
                    Unit: 'Count'
                }]
            }]
        }
    }, dimensionValues, {
        [name]: value
    })));
}

module.exports = {
    info: (message, fields) => logger('info', message, fields),
    warn: (message, fields) => logger('warn', message, fields),
    error: (message, fields) => logger('error', message, fields),
    metric: emitMetric
};
