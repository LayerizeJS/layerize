'use strict';

let assert = require('assert');
let Layerize = require('../lib/index.js');

describe('layerize', () => {

    let layerize;

    before(async () => {

        layerize = new Layerize({ schemas: './test/data/schemas/**/*.json' });

    });

    it('should initiate', async () => {

        return await layerize.initiate({ setup: true });

    }).slow(500);

    it('should create new schema and tables', async () => {

        return await layerize.buildTables({ schemaName: 'public2' });

    });

    it('should create a layer', () => {

        return layerize.layers({ schema: 'public2' });

    });

    describe('layers', () => {

        describe('inserts', () => {

            it('should insert a single record', async () => {

                let layers = layerize.layers({ schema: 'public2' });
                return await layers.insert('users', { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: 2 } ], custom_fields: { pickle: true } });

            });

            it('should insert multiple records', async () => {

                let layers = layerize.layers({ schema: 'public2' });
                return await layers.insert('users', [
                    { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: 2 } ], custom_fields: { pickle: true } },
                    { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Jane', last_name: ' Doe ', username: 'janedoe', password: 'Mypassword1', email: 'jane@doe.com', system_keys: [ { key: '1', value: 2 } ], custom_fields: { pickle: true } }
                ]);

            });

        });

        describe('searches', () => {

            it('should search records', async () => {

                let layers = layerize.layers({ schema: 'public2' });
                return await layers.search('users', { fields: 'id', sort: 'username', filter: ['archived:false', 'first_name:John3&&system_keys:![{"key":"2"}]'] });

            });

            it('should search and return only request fields', async () => {

                let fields = ['id', 'first_name'];

                let layers = layerize.layers({ schema: 'public2' });
                let records = await layers.search('users', { fields });

                assert.equal(true, (Object.keys(records.items[0]).length === fields.length));

            });

            describe('response', () => {

                let layers;
                let records;

                before(async () => {

                    layers = layerize.layers({ schema: 'public2' });
                    records = await layers.search('users');

                });

                it('should have a valid total property', () => {

                    // check if total exists and returns a whole number
                    assert.equal(true, (typeof records.total !== 'undefined' && records.total % 1 === 0 && String(records.total).indexOf('.') === -1));

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
                    assert.equal(true, (typeof records.offset !== 'undefined' && records.offset % 1 === 0 && String(records.offset).indexOf('.') === -1));

                });

                it('should have a valid limit property', () => {

                    // check if limit exists, returns a whole number and is not zero
                    assert.equal(true, (typeof records.limit !== 'undefined' && records.limit % 1 === 0 && String(records.limit).indexOf('.') === -1) && records.limit > 0);

                });

            });

        });

        describe('gets', () => {

            it('should get record', async () => {

                let layers = layerize.layers({ schema: 'public2' });
                let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

                assert.equal(true, (Object.keys(record).length > 0));

            });

            it('should get record with only request fields', async () => {

                let fields = ['id', 'first_name'];

                let layers = layerize.layers({ schema: 'public2' });
                let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { fields });

                assert.equal(true, (Object.keys(record).length === fields.length));

            });

        });

        describe('count', () => {

            it('should get record count', async () => {

                let layers = layerize.layers({ schema: 'public2' });
                let count = await layers.count('users');

                assert.equal(true, (count !== 0));

            });

        });

        describe('deletes', () => {

            it('should delete record', async () => {

                let layers = layerize.layers({ schema: 'public2' });

                let beforeCount = await layers.count('users');

                await layers.delete('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

                let afterCount = await layers.count('users');

                assert.equal(true, (beforeCount !== afterCount));

            });

            it('should delete records by filter', async () => {

                let layers = layerize.layers({ schema: 'public2' });

                let beforeCount = await layers.count('users');

                await layers.deleteByFilter('users', 'system_keys:[{"key":"1"}]');

                let afterCount = await layers.count('users');

                assert.equal(true, (beforeCount !== afterCount));

            });

        });

    });

});
