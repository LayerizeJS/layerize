'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('updates', () => {

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

    it('should update a single record', async () => {

        let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
        user.last_name = 'Updated';

        let updatedUser = await layers.update('users', user, { returnRecord: true });

        assert.equal(user.last_name, updatedUser.last_name);

    }).slow(500).timeout(15000);

    it('should update a many record', async () => {

        let users = await layers.getMany('users', ['a99f0cea-c3df-4619-b023-8c71fee3a9cd', 'b99f0cea-c3df-4619-b023-8c71fee3a9dc']);
        users[0].last_name = 'UpdatedMany';
        users[1].last_name = 'UpdatedManyAgain';

        let updatedUsers = await layers.updateMany('users', users, { returnRecords: true });

        assert.equal(users[0].last_name, updatedUsers[0].last_name);
        assert.equal(users[1].last_name, updatedUsers[1].last_name);

    }).slow(500).timeout(15000);

    it('should update record timestamp property', async () => {

        let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
        user.last_name = 'Updated2';

        let updatedUser = await layers.update('users', user, { returnRecord: true });

        assert.notEqual(user.ts_updated, updatedUser.ts_updated);

    }).slow(500).timeout(15000);

    it('should not update read-only property', async () => {

        let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
        user.account_owner = !user.account_owner;

        let updatedUser = await layers.update('users', user, { returnRecord: true });

        assert.notEqual(user.account_owner, updatedUser.account_owner);

    }).slow(500).timeout(15000);

    it('should update read-only property when ignoreReadOnly set', async () => {

        let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
        user.account_owner = !user.account_owner;

        let updatedUser = await layers.update('users', user, { returnRecord: true, ignoreReadOnly: [ 'account_owner' ] });

        assert.equal(user.account_owner, updatedUser.account_owner);

    }).slow(500).timeout(15000);

    it('should update a single record with elasticSearch disabled', async () => {

        let userRole = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46');
        userRole.name = 'AdminUpdated';

        let response = await layers.update('user_roles', userRole);

        assert.equal(response.success, true);

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});

