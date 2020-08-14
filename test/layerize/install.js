'use strict';

// const assert = require('assert');
const Layerize = global.Layerize;
const layerizeSchemaName = 'layerize_test_2';
const testSchemaName = 'layerize_test_schema_2';

describe('layerize', () => {

    let layerize;

    it('should initiate layerize and install test schema', async () => {

        layerize = new Layerize({ schemas: './test/data/schemas/1.0.0/**/*.json', layerizeSchemaName });

        await layerize.initiate({
            db: {
                client: 'pg',
                connection: {
                    host: 'localhost',
                    user: 'postgres',
                    password: '',
                    database: 'postgres'
                },
                pool: {
                    min: 2,
                    max: 10
                },
                acquireConnectionTimeout: 60000
            },
            cache: {
                host: 'localhost',
                port: 6379
            },
            es: {
                protocol: 'http',
                host: 'localhost',
                port: 9200
            },
            dynamo: {
                accessKeyId: 'ABC123',
                secretAccessKey: 'XXXXX',
                region: 'us-west-2',
                endpoint: 'http://localhost:4566'
            },
            install: true,
            schemaName: testSchemaName
        });

    }).slow(500).timeout(30000);

    it('should initiate and install v1.0.1 updates to all schemas', async () => {

        layerize = new Layerize({ schemas: './test/data/schemas/1.0.1/**/*.json', layerizeSchemaName });

        await layerize.initiate({
            db: {
                client: 'pg',
                connection: {
                    host: 'localhost',
                    user: 'postgres',
                    password: '',
                    database: 'postgres'
                },
                pool: {
                    min: 2,
                    max: 10
                },
                acquireConnectionTimeout: 60000
            },
            cache: {
                host: 'localhost',
                port: 6379
            },
            es: {
                protocol: 'http',
                host: 'localhost',
                port: 9200
            },
            dynamo: {
                accessKeyId: 'ABC123',
                secretAccessKey: 'XXXXX',
                region: 'us-west-2',
                endpoint: 'http://localhost:4566'
            },
            updateAll: true
        });

    }).slow(500).timeout(30000);

    it('should initiate v1.0.2 updates but do not install schemas. ^^ console warning should show above this line ^^', async () => {

        layerize = new Layerize({ schemas: './test/data/schemas/1.0.2/**/*.json', layerizeSchemaName });

        await layerize.initiate({
            db: {
                client: 'pg',
                connection: {
                    host: 'localhost',
                    user: 'postgres',
                    password: '',
                    database: 'postgres'
                },
                pool: {
                    min: 2,
                    max: 10
                },
                acquireConnectionTimeout: 60000
            },
            cache: {
                host: 'localhost',
                port: 6379
            },
            es: {
                protocol: 'http',
                host: 'localhost',
                port: 9200
            },
            dynamo: {
                accessKeyId: 'ABC123',
                secretAccessKey: 'XXXXXX',
                region: 'us-west-2',
                endpoint: 'http://localhost:4566'
            }
        });

    }).slow(500).timeout(30000);

    it('should install v1.0.2 updates on test schema', async () => {

        await layerize.install({ schemaName: testSchemaName });

    }).slow(500).timeout(30000);

    it('should uninstall test schemas and core', async () => {

        await layerize.uninstall({ schemaName: testSchemaName });
        await layerize.uninstall({ layerizeCore: true });

    }).slow(500).timeout(30000);

});
