
'use strict';

const schemas = require('./schemas.json');
const tables = require('./tables.json');

const { layerizeConfigSchemasShared } = require('./shared');

const layerizeConfigSchemas = [
    schemas,
    tables,
    ...layerizeConfigSchemasShared
];

module.exports.layerizeConfigSchemas = layerizeConfigSchemas;
