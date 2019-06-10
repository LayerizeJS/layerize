'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('searches', () => {

    let layers;

    before(async () => {

        layers = layerize.layers({ schemaName: testSchemaName });

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

        // insert new records
        await layers.insert('organization_roles', { id: '5e80d477-ebae-4263-86d0-4498ff13dd0e', name: 'Partner', permissions: [] });
        await layers.insert('organizations', { id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', organization_role_id: '5e80d477-ebae-4263-86d0-4498ff13dd0e', name: 'My Organization', email: 'admin@organization.com', permissions: [] });
        await layers.insert('user_roles', { id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', name: 'Admin', permissions: [] });
        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);

    });

    it('should search records', async () => {

        let results = await layers.search('users', { fields: 'id', sort: 'username', filter: ['archived:false', 'first_name:John'], includes: 'user_role' });

        assert.equal(true, (results.items.length > 0));

    }).slow(500).timeout(15000);

    it('should search records with joins defined', async () => {

        let results = await layers.search('users', { fields: 'users.id, user_roles.name', sort: 'users.username', filter: ['users.archived:false', 'users.first_name:John'], joins: [ 'LEFT JOIN layerize_test_schema.user_roles ON user_roles.id = users.user_role_id' ] });

        assert.equal(true, (results.items.length > 0));

    }).slow(500).timeout(15000);

    it('should search and return only request fields', async () => {

        let fields = ['id', 'first_name', 'user_role.permissions'];

        let records = await layers.search('users', { fields, includes: 'user_role' });

        if (records.items.length > 0) {

            assert.equal(true, (Object.keys(records.items[0]).length === fields.length));

        } else {

            assert.equal(true, false);

        }

    }).slow(500).timeout(15000);

    it('should search records with native db filter', async () => {

        let results = await layers.search('users', { fields: 'id', sort: 'username', filter: { native: true, where: 'archived = \'false\' AND first_name = \'John\'' }, type: 'db', includes: 'user_role' });

        assert.equal(true, (results.items.length > 0));

    }).slow(500).timeout(15000);

    it('should search records with native es filter', async () => {

        let results = await layers.search('users', { fields: 'id', sort: 'username', filter: { native: true, query: { term: { first_name: 'John' } } }, includes: 'user_role' });

        assert.equal(true, (results.items.length > 0));

    }).slow(500).timeout(15000);

    describe('results', () => {

        let layers;
        let records;

        before(async () => {

            layers = layerize.layers({ schemaName: testSchemaName });
            records = await layers.search('users');

        });

        it('should have a valid total property', () => {

            // check if total exists and returns a whole number
            assert.equal(true, (typeof records.total !== 'undefined' && Number.isInteger(records.total) && records.total % 1 === 0 && String(records.total).indexOf('.') === -1));

        }).slow(500).timeout(15000);

        it('should have a valid items property', () => {

            // check if items exists and is an array of objects
            assert.equal(true, (typeof records.items !== 'undefined' && Array.isArray(records.items) && (!Array.isArray(records.items[0]) && typeof records.items[0] === 'object')));

        }).slow(500).timeout(15000);

        it('should have a valid sort property', () => {

            // check if sort exists and is a string that is not blank
            assert.equal(true, (typeof records.sort !== 'undefined') && records.sort !== '' && typeof records.sort === 'string');

        }).slow(500).timeout(15000);

        it('should have a valid offset property', () => {

            // check if offset exists and returns a whole number
            assert.equal(true, (typeof records.offset !== 'undefined' && Number.isInteger(records.offset) && records.offset % 1 === 0 && String(records.offset).indexOf('.') === -1));

        }).slow(500).timeout(15000);

        it('should have a valid limit property', () => {

            // check if limit exists, returns a whole number and is not zero
            assert.equal(true, (typeof records.limit !== 'undefined' && Number.isInteger(records.limit) && records.limit % 1 === 0 && String(records.limit).indexOf('.') === -1) && records.limit > 0);

        }).slow(500).timeout(15000);

    });

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});
