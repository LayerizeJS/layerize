'use strict';

const prepare = require('mocha-prepare');
const Layerize = require('../lib');
const services = require('../lib/services');
const { error } = console;

let layerizeSchemaName = 'layerize_test';
let testSchemaName = 'layerize_test_schema';

const layerize = new Layerize({ schemas: './test/data/schemas/1.0.0/**/*.json', layerizeSchemaName: layerizeSchemaName });

global.services = services;
global.Layerize = Layerize;
global.layerize = layerize;
global.layerizeSchemaName = layerizeSchemaName;
global.testSchemaName = testSchemaName;

prepare(async (done) => {

    try {

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
            },
            permissions: {
                definitionGlob: './test/data/permissions/**/*.json',
                getRole: async (role, { layerize, schemaName }) => {

                    try {

                        let layers = layerize.layers({ schemaName });

                        let objRole = null;

                        try {

                            objRole = await layers.get('user_roles', role);

                            let organization = await layers.get('organizations', objRole.organization_id);

                            objRole = {
                                role,
                                permissions: objRole.permissions,
                                admin: objRole.super_user,
                                adminRole: organization.organization_role_id
                            };

                        } catch (e) {

                            try {

                                objRole = await layers.get('organization_roles', role);

                                objRole = {
                                    role,
                                    permissions: objRole.permissions,
                                    admin: objRole.super_organization
                                };

                            } catch (e) {

                                throw new Error(`role: ${role} could not be found in ${schemaName}`);

                            }

                        }

                        return objRole;

                    } catch (e) {

                        error(JSON.stringify(e));
                        throw e;

                    }

                }
            },
            watchers: [
                {
                    table: 'user_role',
                    on: 'update',
                    action: async (keys, { layerize, schemaName }) => {

                        try {

                            let layers = layerize.layers({ schemaName });
                            return await layers.get('user_role', keys[0]);

                        } catch (e) {

                            throw e;

                        }

                    }
                }
            ]
        });

        await layerize.install({ schemaName: testSchemaName });

    } catch (e) {

        error(JSON.stringify(e));
        throw e;

    }

    done();

}, async (done) => {

    await layerize.uninstall({ schemaName: testSchemaName });
    await layerize.uninstall({ layerizeCore: true });
    done();

});
