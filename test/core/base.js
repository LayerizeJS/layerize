'use strict';

const assert = require('assert');
const Layerize = global.Layerize;
const Base = Layerize.Base;
const layerize = global.layerize;
const layerizeSchemaName = global.layerizeSchemaName;

describe('Base', () => {

    it('should create a instance of the Base class', async () => {

        let tables = new Base({ layerize, schemaName: layerizeSchemaName });

        assert.equal(layerizeSchemaName, tables.schemaName);

    }).slow(500).timeout(15000);

    it('should error create a instance of the Base class', async () => {

        try {

            /* eslint-disable no-unused-vars */
            let tables = new Base();
            /* eslint-enable no-unused-vars */

        } catch (e) {

            assert.equal('A valid instance of Layerize must be passed to the class.', e.message);

        }

    }).slow(500).timeout(15000);

});
