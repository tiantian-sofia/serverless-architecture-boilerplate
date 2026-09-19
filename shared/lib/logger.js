'use strict';

/**
 * Structured JSON logging and CloudWatch Embedded Metrics (EMF).
 *
 * Every log line is a single JSON object so logs can be queried in
 * CloudWatch Logs Insights. Metric() emits an EMF document that
 * CloudWatch turns into a real MetricFilter-free custom metric:
 *
 *   namespace: BooksConsumer
 *   metric:    Processed
 *   dimensions: Outcome (success|duplicate|stale|permanent_failure|
 *                          retryable_failure|dlq|timeout_skip)
 */

const NAMESPACE = process.env.METRICS_NAMESPACE || 'BooksConsumer';

function write(line) {
    process.stdout.write(line + '\n');
}

function build(level, message, fields) {
    const entry = Object.assign({
        level: level,
        message: message,
        time: new Date().toISOString()
    }, fields || {});

    try {
        return JSON.stringify(entry);
    } catch (err) {
        return JSON.stringify({ level: level, message: message });
    }
}

module.exports.info = (message, fields) => write(build('info', message, fields));
module.exports.warn = (message, fields) => write(build('warn', message, fields));
module.exports.error = (message, fields) => write(build('error', message, fields));

/**
 * Emit an EMF metric and a matching structured log line.
 *
 * @param {String} name     Metric name (e.g. Processed, BatchSize)
 * @param {Number} value    Numeric value
 * @param {Object} dims     Dimension map (e.g. { Outcome: 'success' })
 * @param {Object} context  Extra structured log fields
 */
module.exports.metric = (name, value, dims, context) => {
    const dimensions = dims || {};

    const emf = {
        _aws: {
            Timestamp: Date.now(),
            CloudWatchMetrics: [{
                Namespace: NAMESPACE,
                Dimensions: [Object.keys(dimensions)],
                Metrics: [{ Name: name, Unit: 'Count' }]
            }]
        }
    };

    Object.keys(dimensions).forEach(key => {
        emf[key] = dimensions[key];
    });
    emf[name] = value;

    try {
        write(JSON.stringify(emf));
    } catch (err) {
        // Never let metrics break message processing.
    }

    module.exports.info('metric:' + name, Object.assign({
        metric: name,
        value: value
    }, dimensions, context || {}));
};
