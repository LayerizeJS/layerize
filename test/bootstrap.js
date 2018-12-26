'use strict';

const prepare = require('mocha-prepare');
const Layerize = require('../lib');
const services = require('../lib/services');

let layerizeSchemaName = 'layerize_test';
let testSchemaName = 'layerize_test_schema';

const layerize = new Layerize({ schemas: './test/data/schemas/1.0.0/**/*.json', layerizeSchemaName: layerizeSchemaName });

global.services = services;
global.Layerize = Layerize;
global.layerize = layerize;
global.layerizeSchemaName = layerizeSchemaName;
global.testSchemaName = testSchemaName;

prepare(async (done) => {

    // called before loading of test cases
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
        }
    });

    await layerize.install({ schemaName: testSchemaName });
    done();

}, async (done) => {

    await layerize.uninstall({ schemaName: testSchemaName });
    await layerize.uninstall({ layerizeCore: true });
    done();

});
