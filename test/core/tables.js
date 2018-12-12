'use strict';

const assert = require('assert');
const Layerize = global.Layerize;
const Tables = Layerize.Tables;
const layerize = global.layerize;
const layerizeSchemaName = global.layerizeSchemaName;

describe('Tables', () => {

    it('should create a instance of the table class', async () => {

        let tables = new Tables({ layerize, schemaName: layerizeSchemaName });
        await tables.search();

    }).slow(500).timeout(15000);

    it('should error create a instance of the table class', async () => {

        try {

            let tables = new Tables();
            await tables.search();

        } catch (e) {

            assert.equal('A valid instance of Layerize must be passed to the class.', e.message);

        }

    }).slow(500).timeout(15000);

});
