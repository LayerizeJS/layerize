'use strict';

const assert = require('assert');
const Layerize = global.Layerize;
const Schemas = Layerize.Schemas;
const layerize = global.layerize;
const layerizeSchemaName = global.layerizeSchemaName;

describe('Schemas', () => {

    it('should create a instance of the Schemas class', async () => {

        let tables = new Schemas({ layerize, schemaName: layerizeSchemaName });
        await tables.search();

    }).slow(500).timeout(15000);

    it('should error create a instance of the Schemas class', async () => {

        try {

            let tables = new Schemas();
            await tables.search();

        } catch (e) {

            assert.equal('A valid instance of Layerize must be passed to the class.', e.message);

        }

    }).slow(500).timeout(15000);

});
