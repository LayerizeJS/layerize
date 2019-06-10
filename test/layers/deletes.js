'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('deletes', () => {

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

    it('should delete record', async () => {

        let layers = layerize.layers({ schemaName: testSchemaName });

        let beforeCount = await layers.count('users');

        await layers.delete('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

        let afterCount = await layers.count('users');

        assert.equal(true, (beforeCount !== afterCount));

    }).slow(500).timeout(15000);

    it('should delete records by filter', async () => {

        let layers = layerize.layers({ schemaName: testSchemaName });

        let beforeCount = await layers.count('users');

        await layers.deleteByFilter('users', 'system_keys:[{"key":"1"}]');

        let afterCount = await layers.count('users');

        assert.equal(true, (beforeCount !== afterCount));

    }).slow(500).timeout(15000);

    it('should delete others records', async () => {

        let layers = layerize.layers({ schemaName: testSchemaName });

        let beforeCount = await layers.count('user_roles');

        await layers.delete('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46');

        let afterCount = await layers.count('user_roles');

        assert.equal(true, (beforeCount !== afterCount));

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});

