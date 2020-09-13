'use strict';

const user_roles = require('./user_roles.json');
const users = require('./users.json');

const layerizeTestConfigPermissions = [
    user_roles,
    users
];

module.exports.layerizeTestConfigPermissions = layerizeTestConfigPermissions;
