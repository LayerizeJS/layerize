'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('inserts', () => {

    let layers = layerize.layers({ schemaName: testSchemaName });

    before(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

        // insert new records
        await layers.insert('organization_roles', { id: '5e80d477-ebae-4263-86d0-4498ff13dd0e', name: 'Partner', permissions: [] });
        await layers.insert('organizations', { id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', organization_role_id: '5e80d477-ebae-4263-86d0-4498ff13dd0e', name: 'My Organization', email: 'admin@organization.com', permissions: [] });

    });

    it('should insert a single record', async () => {

        let beforeCount = await layers.count('users');

        await layers.insert('user_roles', { id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', name: 'Admin', permissions: [] });
        await layers.insert('users', { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } });

        let afterCount = await layers.count('users');

        assert.equal(true, (afterCount === beforeCount + 1));

    }).slow(500).timeout(15000);

    it('should error on inserting a single record', async () => {

        try {

            await layers.insert('users', { user_role_id: 'd8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } });

        } catch (e) {

            assert.equal(true, e[1].errors[0].message.indexOf('__nonTransactionProcessing') > -1);

        }

    }).slow(500).timeout(15000);

    it('should insert multiple records', async () => {

        let beforeCount = await layers.count('users');

        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Jane', last_name: ' Doe ', username: 'janedoe', password: 'Mypassword1', email: 'jane@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);
        let afterCount = await layers.count('users');

        assert.equal(true, (afterCount === beforeCount + 2));

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});

