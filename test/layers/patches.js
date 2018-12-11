'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('patches', () => {

    let layers = layerize.layers({ schemaName: testSchemaName });

    before(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_role');

        // insert new records
        await layers.insert('user_role', { id: 'a8988288-988a-412a-9127-e51a284e2b46', name: 'Admin', permissions: {} });
        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);

    });

    it('should patch a single record', async () => {

        let user = {
            last_name: 'Patched'
        };

        let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

        assert.equal(user.last_name, updatedUser.last_name);

    }).slow(500).timeout(15000);

    it('should patch a many records', async () => {

        let users = [
            {
                id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd',
                last_name: 'PatchedMany'
            },
            {
                id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc',
                first_name: 'MaryMany'
            }
        ];

        let updatedUsers = await layers.patchMany('users', users, { returnRecords: true });

        assert.equal(users[0].last_name, updatedUsers[0].last_name);
        assert.equal(users[1].first_name, updatedUsers[1].first_name);

    }).slow(500).timeout(15000);

    it('should patch record timestamp property', async () => {

        let user = {
            last_name: 'Patched2'
        };

        let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

        assert.notEqual(user.ts_updated, updatedUser.ts_updated);

    }).slow(500).timeout(15000);

    it('should not patch read-only property', async () => {

        let user = {
            account_owner: true
        };

        let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });

        assert.notEqual(user.account_owner, updatedUser.account_owner);

    }).slow(500).timeout(15000);

    it('should patch read-only property when ignoreReadOnly set', async () => {

        let user = {
            account_owner: true
        };

        let updatedUser = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true, ignoreReadOnly: [ 'account_owner' ] });

        assert.equal(user.account_owner, updatedUser.account_owner);

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_role');

    });

});

