'use strict';

const lambdaTimeoutSeconds = Number(process.env.BOOKS_CONSUMER_TIMEOUT_SECONDS || 60);
const maximumBatchingWindowSeconds = Number(process.env.SQS_MAXIMUM_BATCHING_WINDOW_SECONDS || 5);
const defaultVisibilityTimeoutSeconds =
    (lambdaTimeoutSeconds * 6) + maximumBatchingWindowSeconds + 5;

module.exports = {
    batchSize: Number(process.env.SQS_BATCH_SIZE || 10),
    maximumBatchingWindowSeconds,
    maximumConcurrency: Number(process.env.SQS_MAXIMUM_CONCURRENCY || 5),
    lambdaTimeoutSeconds,
    visibilityTimeoutSeconds: Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS || defaultVisibilityTimeoutSeconds),
    maxReceiveCount: Number(process.env.SQS_MAX_RECEIVE_COUNT || 5)
};
