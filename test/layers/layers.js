'use strict';

// const assert = require('assert');
const layerize = global.layerize;
const testSchemaName = global.testSchemaName;

describe('layers', () => {

    it('should create a layer', () => {

        return layerize.layers({ schemaName: testSchemaName });

    });

});

