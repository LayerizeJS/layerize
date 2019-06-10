'use strict';

const assert = require('assert');
const Layerize = global.Layerize;
const Permissions = Layerize.Permissions;
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('Permissions', () => {

    let layers = layerize.layers({ schemaName: testSchemaName });

    before(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

        // insert new records
        await layers.insert('organization_roles', {
            id: '5e80d477-ebae-4263-86d0-4498ff13dd0e',
            name: 'Partner',
            permissions: [
                {
                    resource: 'users:user_roles',
                    action: 'create',
                    properties: [ '*' ]
                },
                {
                    resource: 'users:user_roles',
                    action: 'update',
                    properties: [ '*' ]
                },
                {
                    resource: 'users:user_roles',
                    action: 'read',
                    properties: [
                        '*',
                        {
                            property: 'super_user',
                            condition: {
                                type: 'fact',
                                property: 'name',
                                operator: 'EQUALS',
                                value: 'Admin'
                            }
                        }
                    ],
                    condition: {
                        type: 'all',
                        children: [
                            {
                                type: 'fact',
                                property: 'name',
                                operator: 'EQUALS',
                                value: 'Admin'
                            }
                        ]
                    }
                }
            ]
        });
        await layers.insert('organizations', { id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', organization_role_id: '5e80d477-ebae-4263-86d0-4498ff13dd0e', name: 'My Organization', email: 'admin@organization.com', permissions: [] });
        await layers.insert('user_roles', {
            id: 'a8988288-988a-412a-9127-e51a284e2b46',
            organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c',
            name: 'Admin',
            permissions: [
                {
                    resource: 'users:user_roles',
                    action: 'create',
                    properties: [ '*' ]
                },
                {
                    resource: 'users:user_roles',
                    action: 'read',
                    properties: [ '*' ]
                }
            ]
        });
        await layers.insert('user_roles', { id: 'b7877288-788a-312a-8128-f41a284e2b34', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', name: 'Manger', permissions: [ { resource: 'users:user_roles', action: 'create', properties: [ '*' ] } ] });
        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', organization_id: '49f89fe4-9b8b-45aa-b3da-4f11711c8c1c', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);

    });

    it('should create a instance of the Permissions class', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        assert.equal(testSchemaName, permissions.schemaName);

    }).slow(500).timeout(15000);

    it('should error creating an instance of the Permissions class', async () => {

        try {

            /* eslint-disable no-unused-vars */
            let permissions = new Permissions();
            /* eslint-enable no-unused-vars */

        } catch (e) {

            assert.equal('A valid instance of Layerize must be passed to the class.', e.message);

        }

    }).slow(500).timeout(15000);

    it('should return a permission object with granted is true, when a single passed role is true', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ],
            resource: 'users:user_roles',
            action: 'update'
        });

        assert.equal(true, permission.granted);

    }).slow(500).timeout(15000);

    it('should return a permission object with granted is true, when all passed roles are true', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: ['5e80d477-ebae-4263-86d0-4498ff13dd0e', 'a8988288-988a-412a-9127-e51a284e2b46'],
            resource: 'users:user_roles',
            action: 'create'
        });

        assert.equal(true, permission.granted);

    }).slow(500).timeout(15000);

    it('should not return a property that has been restricted', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46', { permission });

        assert.equal(undefined, userRole.super_user);

    }).slow(500).timeout(15000);

    it('should not return a property that has been restricted, even when requested.', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.get('user_roles', 'a8988288-988a-412a-9127-e51a284e2b46', { fields: ['name', 'super_user'], permission });

        assert.equal(undefined, userRole.super_user);

    }).slow(500).timeout(15000);

    it('should return one record, with native DB filter', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.search('user_roles', { filter: { native: true, where: 'name != \'Manager2\'' }, permission });

        assert.equal(true, (userRole.items.length === 1));

    }).slow(500).timeout(15000);

    it('should return one record', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.search('user_roles', { permission });

        assert.equal(true, (userRole.items.length === 1));

    }).slow(500).timeout(15000);

    it('should return one record, when one of the role has a condition against it', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: ['5e80d477-ebae-4263-86d0-4498ff13dd0e', 'a8988288-988a-412a-9127-e51a284e2b46'],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.search('user_roles', { permission });

        assert.equal(1, userRole.items.length);

    }).slow(500).timeout(15000);

    it('should return two record', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: [ 'a8988288-988a-412a-9127-e51a284e2b46' ],
            resource: 'users:user_roles',
            action: 'read'
        });

        let userRole = await layers.search('user_roles', { permission });

        assert.equal(2, userRole.items.length);

    }).slow(500).timeout(15000);

    it('should return a permission object with granted is false, when any one role is false', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: ['5e80d477-ebae-4263-86d0-4498ff13dd0e', 'a8988288-988a-412a-9127-e51a284e2b46'],
            resource: 'users:user_roles',
            action: 'update'
        });

        assert.equal(false, permission.granted);

    }).slow(500).timeout(15000);

    it('should return a permission object with granted is false', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.allowed({
            role: ['5e80d477-ebae-4263-86d0-4498ff13dd0e', 'a8988288-988a-412a-9127-e51a284e2b46'],
            resource: 'users:user_roles',
            action: 'nonExistingAction'
        });

        assert.equal(false, permission.granted);

    }).slow(500).timeout(15000);

    it('should return a access list object', async () => {

        let permissions = new Permissions({ layerize, schemaName: testSchemaName });

        let permission = await permissions.accessList({
            role: [ '5e80d477-ebae-4263-86d0-4498ff13dd0e' ]
        });

        assert.equal(true, Object.keys(permission).length > 0);

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_roles');
        await layers.deleteByFilter('organizations');
        await layers.deleteByFilter('organization_roles');

    });

});
