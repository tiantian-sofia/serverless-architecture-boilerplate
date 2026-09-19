'use strict';

const fs = require('fs');
const path = require('path');
const chai = require('chai');
const expect = chai.expect;
const YAML = require('yamljs');

const repoRoot = path.resolve(__dirname, '../../..');
const serverlessYaml = fs.readFileSync(path.join(repoRoot, 'serverless.yml'), 'utf8');
const workersYaml = fs.readFileSync(path.join(repoRoot, 'handlers/books-workers.yml'), 'utf8');
const consumerConfig = require(path.join(repoRoot, 'configs/books-consumer.js'));

describe('books consumer infrastructure', () => {
    let workers;

    before(() => {
        workers = YAML.parse(workersYaml);
    });

    it('uses SQS Event Source Mapping instead of a scheduled trigger', () => {
        expect(workers['books-consumer'].events).to.have.length(1);
        expect(workers['books-consumer'].events[0].sqs).to.be.an('object');
        expect(workers['books-consumer'].events[0].schedule).to.equal(undefined);
        expect(serverlessYaml).to.not.contain('serverless-offline-scheduler');
        expect(serverlessYaml).to.not.contain('rate(1 minute)');
    });

    it('configures batch window, concurrency and partial batch failures', () => {
        const sqsEvent = workers['books-consumer'].events[0].sqs;

        expect(sqsEvent.batchSize).to.equal('${self:custom.booksConsumer.batchSize}');
        expect(sqsEvent.maximumBatchingWindow).to.equal('${self:custom.booksConsumer.maximumBatchingWindowSeconds}');
        expect(sqsEvent.maximumConcurrency).to.equal('${self:custom.booksConsumer.maximumConcurrency}');
        expect(sqsEvent.functionResponseType).to.equal('ReportBatchItemFailures');

        expect(consumerConfig.batchSize).to.be.a('number');
        expect(consumerConfig.maximumBatchingWindowSeconds).to.be.a('number');
        expect(consumerConfig.maximumConcurrency).to.be.a('number');
        expect(consumerConfig.maxReceiveCount).to.be.a('number');
    });

    it('defines a DLQ redrive policy with configurable maxReceiveCount', () => {
        expect(serverlessYaml).to.contain('BooksDeadLetterQueue:');
        expect(serverlessYaml).to.contain('RedrivePolicy:');
        expect(serverlessYaml).to.contain('deadLetterTargetArn:');
        expect(serverlessYaml).to.contain('- BooksDeadLetterQueue');
        expect(serverlessYaml).to.contain('- Arn');
        expect(serverlessYaml).to.contain('maxReceiveCount: ${self:custom.booksConsumer.maxReceiveCount}');
    });

    it('stores event idempotency and per-hashKey version state in DynamoDB', () => {
        expect(serverlessYaml).to.contain('BooksEventState:');
        expect(serverlessYaml).to.contain('BooksEventIdempotency:');
        expect(serverlessYaml).to.contain('dynamodb:TransactWriteItems');
    });

    it('sets SQS visibility timeout beyond Lambda timeout with batching window included', () => {
        expect(consumerConfig.visibilityTimeoutSeconds).to.be.at.least(
            (consumerConfig.lambdaTimeoutSeconds * 6) + consumerConfig.maximumBatchingWindowSeconds
        );
    });
});
