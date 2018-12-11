'use strict';

const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('inserts', () => {

    let layers = layerize.layers({ schemaName: testSchemaName });

    before(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_role');

    });

    it('should insert a single record', async () => {

        let beforeCount = await layers.count('users');

        await layers.insert('user_role', { id: 'a8988288-988a-412a-9127-e51a284e2b46', name: 'Admin', permissions: {} });
        await layers.insert('users', { id: 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } });

        let afterCount = await layers.count('users');

        assert.equal(true, (afterCount === beforeCount + 1));

    }).slow(500).timeout(15000);

    it('should insert multiple records', async () => {

        let beforeCount = await layers.count('users');

        await layers.insertMany('users', [
            { id: 'b99f0cea-c3df-4619-b023-8c71fee3a9dc', user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Mary', last_name: ' Doe ', username: 'marydoe', password: 'Mypassword1', email: 'mary@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } },
            { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'Jane', last_name: ' Doe ', username: 'janedoe', password: 'Mypassword1', email: 'jane@doe.com', system_keys: [ { key: '1', value: '2' } ], custom_fields: { pickle: true } }
        ]);
        let afterCount = await layers.count('users');

        assert.equal(true, (afterCount === beforeCount + 2));

    }).slow(500).timeout(15000);

    after(async () => {

        // remove all records from table
        await layers.deleteByFilter('users');
        await layers.deleteByFilter('user_role');

    });

});

