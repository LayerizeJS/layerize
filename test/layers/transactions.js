'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('transactions', () => {

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

    it('should insert a records', async () => {

        let beforeCount = await layers.count('users');

        let transaction = layers.transaction();
        await transaction.insert('users', [
            { id: 'b55f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Santa', last_name: 'Clause', username: 'santa', password: 'Mypassword1', email: 'santa@email.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { id: 'd44f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Saint', last_name: 'Nick', username: 'snick', password: 'Mypassword1', email: 'snick@email.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);
        await transaction.commit();

        let afterCount = await layers.count('users');

        assert.equal((beforeCount + 2), afterCount);

    }).slow(500).timeout(15000);

    it('should get record', async () => {

        let transaction = layers.transaction();
        let record = await transaction.get('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd');

        assert.equal(true, (Object.keys(record).length > 0));

    }).slow(500).timeout(15000);

    it('should get and lock record for updating', async () => {

        let transaction = layers.transaction();

        let user = {};
        try {

            user = await transaction.get('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd', { forUpdate: true });
            user.first_name = 'Santa2';
            await transaction.update('users', user);

            await transaction.commit();

        } catch (e) {

            await transaction.rollback();
            throw e;

        }

        assert.equal(true, (Object.keys(user).length > 0));

    }).slow(500).timeout(5000);

    it('should patch a single record', async () => {

        let transaction = layers.transaction();

        let user = {
            last_name: 'Patched34'
        };

        await transaction.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true });
        await transaction.commit();

        let updatedUser = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

        assert.equal(user.last_name, updatedUser.last_name);

    }).slow(500).timeout(15000);

    it('should patch a single record with elasticSearch disabled', async () => {

        let transaction = layers.transaction();

        let userRole = {
            name: 'AdminPatched'
        };

        await transaction.patch('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46', userRole, { returnRecord: true });
        await transaction.commit();

        let patchedUserRole = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46');

        assert.equal(userRole.name, patchedUserRole.name);

    }).slow(500).timeout(15000);

    it('should patch a many records', async () => {

        let transaction = layers.transaction();

        let users = [
            {
                id: 'b55f0cea-c3df-4619-b023-8c71fee3a9cd',
                first_name: 'Santa2Many'
            },
            {
                id: 'd44f0cea-c3df-4619-b023-8c71fee3a9dc',
                last_name: 'NickMany'
            }
        ];

        await transaction.patchMany('users', users);
        await transaction.commit();

        let updatedUsers = await layers.getMany('users', ['b55f0cea-c3df-4619-b023-8c71fee3a9cd', 'd44f0cea-c3df-4619-b023-8c71fee3a9dc']);

        assert.equal(users[0].first_name, updatedUsers[0].first_name);
        assert.equal(users[1].last_name, updatedUsers[1].last_name);

    }).slow(500).timeout(15000);

    it('should records patch by filter', async () => {

        let transaction = layers.transaction();

        let user = {
            last_name: 'PatchedByFilter'
        };

        await transaction.patchByFilter('users', 'id:a99f0cea-c3df-4619-b023-8c71fee3a9cd', user);
        await transaction.commit();

        let updatedUser = await transaction.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

        assert.equal(user.last_name, updatedUser.last_name);

    }).slow(500).timeout(15000);

    it('should update a single record', async () => {

        let transaction = layers.transaction();

        let user = await transaction.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
        user.last_name = 'UpdatedTransaction';

        await transaction.update('users', user);
        await transaction.commit();

        let updatedUser = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');

        assert.equal(user.last_name, updatedUser.last_name);

    }).slow(500).timeout(15000);

    it('should update a single record with elasticSearch disabled', async () => {

        let transaction = layers.transaction();

        let userRole = await transaction.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46');
        userRole.name = 'AdminUpdatedTransaction';

        await transaction.update('user_roles', userRole);
        await transaction.commit();

        let updatedUserRole = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46');

        assert.equal(userRole.name, updatedUserRole.name);

    }).slow(500).timeout(15000);

    it('should update a many record', async () => {

        let transaction = layers.transaction();

        let users = [];
        try {

            users = await transaction.getMany('users', ['b55f0cea-c3df-4619-b023-8c71fee3a9cd', 'd44f0cea-c3df-4619-b023-8c71fee3a9dc'], { forUpdate: true });
            users[0].last_name = 'Santa2UpdatedMany';
            users[1].last_name = 'NickUpdatedMany';

            await transaction.updateMany('users', users);
            await transaction.commit();

        } catch (e) {

            await transaction.rollback();
            throw e;

        }

        let updatedUsers = await transaction.getMany('users', ['b55f0cea-c3df-4619-b023-8c71fee3a9cd', 'd44f0cea-c3df-4619-b023-8c71fee3a9dc']);

        assert.equal(users[0].last_name, updatedUsers[0].last_name);
        assert.equal(users[1].last_name, updatedUsers[1].last_name);

    }).slow(500).timeout(15000);

    it('should delete record', async () => {

        let beforeCount = await layers.count('users');

        let transaction = layers.transaction();
        await transaction.delete('users', 'b55f0cea-c3df-4619-b023-8c71fee3a9cd');
        await transaction.commit();

        let afterCount = await layers.count('users');

        assert.equal((beforeCount - 1), afterCount);

    }).slow(500).timeout(15000);

    it('should delete records by filter', async () => {

        let beforeCount = await layers.count('users');

        let transaction = layers.transaction();
        await transaction.deleteByFilter('users', 'last_name:NickUpdatedMany');
        await transaction.commit();

        let afterCount = await layers.count('users');

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

