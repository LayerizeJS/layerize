'use strict';

const _config = require('./_config.json');
const user_roles = require('./user_roles.json');
const users = require('./users.json');

const { layerizeTestConfigSchemasShared1_0_2 } = require('./shared');

const layerizeTestConfigSchemas1_0_2 = [
    _config,
    user_roles,
    users,
    ...layerizeTestConfigSchemasShared1_0_2
];

module.exports.layerizeTestConfigSchemas1_0_2 = layerizeTestConfigSchemas1_0_2;
