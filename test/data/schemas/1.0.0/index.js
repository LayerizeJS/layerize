'use strict';

const _config = require('./_config.json');
const organization_roles = require('./organization_roles.json');
const organizations = require('./organizations.json');
const user_roles = require('./user_roles.json');
const users = require('./users.json');

const { layerizeTestConfigSchemasShared1_0_0 } = require('./shared');

const layerizeTestConfigSchemas1_0_0 = [
    _config,
    organization_roles,
    organizations,
    user_roles,
    users,
    ...layerizeTestConfigSchemasShared1_0_0
];

module.exports.layerizeTestConfigSchemas1_0_0 = layerizeTestConfigSchemas1_0_0;
