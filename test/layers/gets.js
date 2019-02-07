'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('gets', () => {

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
        await layers.insert('user_roles', { id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', name: 'Admin', permissions: [] });
        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);

    });

    it('should get record', async () => {

        let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { includes: 'user_role' });

        assert.equal(true, (Object.keys(record).length > 0));

    }).slow(500).timeout(15000);

    it('should get record with only request fields', async () => {

        let fields = ['id', 'first_name', 'user_role.permissions'];

        let record = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { fields, includes: 'user_role' });

        assert.equal(true, (Object.keys(record).length === fields.length));

    }).slow(500).timeout(15000);

    it('should get record with reference includes', async () => {

        let record = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46', { includes: 'users' });
        assert.equal(true, (record.users.length > 0));

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});
