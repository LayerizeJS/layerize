'use strict';

const assert = require('assert');
const Layerize = global.Layerize;
const Crud = Layerize.Crud;
const layerize = global.layerize;
const layerizeSchemaName = global.layerizeSchemaName;

describe('Crud', () => {

    it('should create a instance of the Base class', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName });

        assert.equal(layerizeSchemaName, crud.schemaName);

    }).slow(500).timeout(15000);

    it('should error create a instance of the Base class', async () => {

        try {

            /* eslint-disable no-unused-vars */
            let tables = new Crud();
            /* eslint-enable no-unused-vars */

        } catch (e) {

            assert.equal('A valid instance of Layerize must be passed to the class.', e.message);

        }

    }).slow(500).timeout(15000);

    it('should insert record', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName, table: 'schemas' });

        let record = await crud.insert({
            id: 'a8988288-988a-412a-9127-e51a284e2b46',
            name: 'XXXXXXXXXXXX',
            version: '1.0.0',
            hash: 'XXXXXXX'
        });

        assert.equal(true, Object.keys(record).length > 0);

    }).slow(500).timeout(15000);

    it('should get record', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName, table: 'schemas' });

        let record = await crud.get('a8988288-988a-412a-9127-e51a284e2b46');

        assert.equal(true, Object.keys(record).length > 0);

    }).slow(500).timeout(15000);

    it('should patch record', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName, table: 'schemas' });

        let record = await crud.patch('a8988288-988a-412a-9127-e51a284e2b46', {
            name: 'YYYYYYYYYY'
        });

        assert.equal(true, Object.keys(record).length > 0);
        assert.equal(record.name, 'YYYYYYYYYY');

    }).slow(500).timeout(15000);

    it('should update record', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName, table: 'schemas' });

        let record = await crud.get('a8988288-988a-412a-9127-e51a284e2b46');

        record.name = 'ZZZZZZZZZZ';

        record = await crud.update('a8988288-988a-412a-9127-e51a284e2b46', record);

        assert.equal(true, Object.keys(record).length > 0);
        assert.equal(record.name, 'ZZZZZZZZZZ');

    }).slow(500).timeout(15000);

    it('should delete record', async () => {

        let crud = new Crud({ layerize, schemaName: layerizeSchemaName, table: 'schemas' });

        let response = await crud.delete('a8988288-988a-412a-9127-e51a284e2b46');

        assert.equal(true, response.success);

    }).slow(500).timeout(15000);

});
