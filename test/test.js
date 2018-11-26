'use strict';

const assert = require('assert');
const Layerize = require('../lib');

describe('layerize', () => {

    let layerize;
    let layerizeSchemaName = 'layerize_test';
    let testSchemaName = 'layerize_test_schema';

    before(async () => {

        layerize = new Layerize({ schemas: './test/data/schemas/**/*.json', layerizeSchemaName });

    });

    it('should initiate', async () => {

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

        assert.equal(true, (typeof layerize.dbSchemas[layerizeSchemaName] !== 'undefined'));

    }).slow(500).timeout(15000);

    describe(`schema creation '${testSchemaName}'`, () => {

        it('should create new schema', async () => {

            await layerize.install({ schemaName: testSchemaName });
            assert.equal(true, (typeof layerize.dbSchemas[testSchemaName] !== 'undefined'));

        });

        it('should have test table called \'users\'', async () => {

            assert.equal(true, (typeof layerize.dbSchemas[testSchemaName] !== 'undefined'));

        });

    });

    describe('layers', () => {

        it('should create a layer', () => {

            return layerize.layers({ schemaName: testSchemaName });

        });

        describe('inserts', () => {

            it('should insert a single record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                return await layers.insert('users', { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } });

            });

            it('should insert multiple records', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                return await layers.insert('users', [
                    { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
                    { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Jane', last_name: ' Doe ', username: 'janedoe', password: 'Mypassword1', email: 'jane@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
                ]);

            });

        });

        describe('searches', () => {

            it('should search records', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                return await layers.search('users', { fields: 'id', sort: 'username', filter: ['archived:false', 'first_name:John'] });

            });

            it('should search and return only request fields', async () => {

                let fields = ['id', 'first_name'];

                let layers = layerize.layers({ schemaName: testSchemaName });
                let records = await layers.search('users', { fields });

                assert.equal(true, (Object.keys(records.items[0]).length === fields.length));

            });

            describe('response', () => {

                let layers;
                let records;

                before(async () => {

                    layers = layerize.layers({ schemaName: testSchemaName });
                    records = await layers.search('users');

                });

                it('should have a valid total property', () => {

                    // check if total exists and returns a whole number
                    assert.equal(true, (typeof records.total !== 'undefined' && Number.isInteger(records.total) && records.total % 1 === 0 && String(records.total).indexOf('.') === -1));

                });

                it('should have a valid items property', () => {

                    // check if items exists and is an array of objects
                    assert.equal(true, (typeof records.items !== 'undefined' && Array.isArray(records.items) && (!Array.isArray(records.items[0]) && typeof records.items[0] === 'object')));

                });

                it('should have a valid sort property', () => {

                    // check if sort exists and is a string that is not blank
                    assert.equal(true, (typeof records.sort !== 'undefined') && records.sort !== '' && typeof records.sort === 'string');

                });

                it('should have a valid offset property', () => {

                    // check if offset exists and returns a whole number
                    assert.equal(true, (typeof records.offset !== 'undefined' && Number.isInteger(records.offset) && records.offset % 1 === 0 && String(records.offset).indexOf('.') === -1));

                });

                it('should have a valid limit property', () => {

                    // check if limit exists, returns a whole number and is not zero
                    assert.equal(true, (typeof records.limit !== 'undefined' && Number.isInteger(records.limit) && records.limit % 1 === 0 && String(records.limit).indexOf('.') === -1) && records.limit > 0);

                });

            });

        });

        describe('gets', () => {

            it('should get record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

                assert.equal(true, (Object.keys(record).length > 0));

            });

            it('should get record with only request fields', async () => {

                let fields = ['id', 'first_name'];

                let layers = layerize.layers({ schemaName: testSchemaName });
                let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { fields });

                assert.equal(true, (Object.keys(record).length === fields.length));

            });

        });

        describe('count', () => {

            it('should get record count', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                let count = await layers.count('users');

                assert.equal(true, (count !== 0));

            });

            it('should be a valid numeric value', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                let count = await layers.count('users');

                assert.equal(true, (Number.isInteger(count) && count % 1 === 0 && String(count).indexOf('.') === -1));

            });

        });

        describe('patches', () => {

            it('should patch a single record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = {
                    last_name: 'Patched'
                };

                let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.equal(user.last_name, updatedUser.last_name);

            });

            it('should patch record timestamp property', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = {
                    last_name: 'Patched2'
                };

                let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.notEqual(user.ts_updated, updatedUser.ts_updated);

            });

            it('should not patch read-only property', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = {
                    account_owner: true
                };

                let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.notEqual(user.account_owner, updatedUser.account_owner);

            });

            it('should patch read-only property when ignoreReadOnly set', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = {
                    account_owner: true
                };

                let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true, ignoreReadOnly: [ 'account_owner' ] });

                assert.equal(user.account_owner, updatedUser.account_owner);

            });

        });

        describe('updates', () => {

            it('should update a single record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
                user.last_name = 'Updated';

                let updatedUser = await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.equal(user.last_name, updatedUser.last_name);

            });

            it('should update record timestamp property', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
                user.last_name = 'Updated2';

                let updatedUser = await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.notEqual(user.ts_updated, updatedUser.ts_updated);

            });

            it('should not update read-only property', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
                user.account_owner = !user.account_owner;

                let updatedUser = await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

                assert.notEqual(user.account_owner, updatedUser.account_owner);

            });

            it('should update read-only property when ignoreReadOnly set', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
                user.account_owner = !user.account_owner;

                let updatedUser = await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true, ignoreReadOnly: [ 'account_owner' ] });

                assert.equal(user.account_owner, updatedUser.account_owner);

            });

        });

        describe('transactions', () => {

            it('should insert a records', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let beforeCount = await layers.count('users');

                let transaction = layers.transaction();
                await transaction.insert('users', [
                    { id: 'b55f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Santa', last_name: 'Clause', username: 'santa', password: 'Mypassword1', email: 'santa@email.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
                    { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Saint', last_name: 'Nick', username: 'snick', password: 'Mypassword1', email: 'snick@email.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
                ]);
                await transaction.commit();

                let afterCount = await layers.count('users');

                assert.equal((beforeCount + 2), afterCount);

            });

            it('should get record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let transaction = layers.transaction();
                let record = await transaction.get('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd');

                assert.equal(true, (Object.keys(record).length > 0));

            });

            it('should get and lock record for updating', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });
                let transaction = layers.transaction();

                let user = await transaction.get('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd', { forUpdate: true });
                user.first_name = 'Santa2';
                await layers.update('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd', user);

                await transaction.commit();

                assert.equal(true, (Object.keys(user).length > 0));

            }).slow(500).timeout(5000);

            it('should delete record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let beforeCount = await layers.count('users');

                let transaction = layers.transaction();
                await transaction.delete('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd');
                await transaction.commit();

                let afterCount = await layers.count('users');

                assert.equal((beforeCount - 1), afterCount);

            });

        });

        describe('deletes', () => {

            it('should delete record', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let beforeCount = await layers.count('users');

                await layers.delete('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

                let afterCount = await layers.count('users');

                assert.equal(true, (beforeCount !== afterCount));

            });

            it('should delete records by filter', async () => {

                let layers = layerize.layers({ schemaName: testSchemaName });

                let beforeCount = await layers.count('users');

                await layers.deleteByFilter('users', 'system_keys:[{"key":"1"}]');

                let afterCount = await layers.count('users');

                assert.equal(true, (beforeCount !== afterCount));

            });

        });

    });

    describe('test cleanup', () => {

        it(`should delete '${testSchemaName}' database schema`, async () => {

            await layerize.uninstall({ schemaName: testSchemaName });
            assert.equal(true, (typeof layerize.dbSchemas[testSchemaName] === 'undefined'));

        });

        it(`should delete '${layerizeSchemaName}' database schema`, async () => {

            await layerize.uninstall({ layerizeCore: true });
            assert.equal(true, (typeof layerize.dbSchemas[layerizeSchemaName] === 'undefined'));

        });

    });

});
