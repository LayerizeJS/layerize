'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:permissions');
const AccessControl = require('./access-control');
const Rules = require('./rules');
const { errors } = require('../utils');
const glob = require('glob');
const fs = require('fs');
const extend = require('extend');

/**
 * The Permissions class is a handling role based permissions.
 */
class Permissions {

    /**
     * Create a Permission.
     * @param {object} config - available config
     */
    constructor ({ layerize, schemaName = 'public' } = {}) {

        if (typeof layerize !== 'object' || layerize === null || typeof layerize === 'undefined') {

            throw new Error('A valid instance of Layerize must be passed to the class.');

        }

        /**
         * Name of the schema being used within the layer
         * @member {string}
         * */
        this.schemaName = schemaName;

        /**
         * the instance of layerize
         * @member {Layerize}
         * */
        this.layerize = layerize;

        /**
         * The initialized debug instance
         * @member {debug}
         * */
        this.debug = debug;

        /**
         * The prepend name of the cache key
         * @member {string}
         * */
        this.cacheKey = `LAYERIZE:PERMISSIONS:${this.schemaName.toUpperCase()}`;

        /**
         * The error utility
         * @member {object}
         * */
        this.error = errors;

        /**
         * The cache utility to use
         * @member {object}
         * */
        this.cache = layerize.cache;

        /**
         * The permission definition
         * @member {object}
         * */
        this.definitions = this.layerize.permissions.definitions;

        /**
         * The hash of the permission definition
         * @member {string}
         * */
        this.hash = this.layerize.permissions.hash;

        this.rules = new Rules();

    }

    /**
     * check if permissions object is authorized and returns true or fails based on granted property
     * @param {permission} permission - a permissions object returned from allowed
     * @returns {Promise<authorization>} returns true or false is valid and allowed
     */
    async authorized (permission = {}) {

        this.debug('authorized');

        try {

            let obj = {
                conditions: [],
                properties: []
            };

            if (Object.keys(permission).length > 0) {

                if (permission.granted === false) {

                    throw new Error(`You do not have permission to execute '${permission.action}' under resource '${permission.resource}'.`);

                }

                for (let i = 0; i < permission._.length; i++) {

                    let _ = permission._[i];
                    obj.conditions.push(_.condition);
                    obj.properties.push(_.properties);

                }

            } else {

                obj.properties.push([ '*' ]);

            }

            return obj;

        } catch (error) {

            throw this.error.handle({ error, caller: 'authorized' });

        }

    }

    /**
     * check if records are allowed to be viewed
     * @param {authorization} authorization - a permissions object returned from allowed
     * @param {object|array} data - an object or array of objects to check for authorization
     * @returns {Promise<boolean>} returns true or false is valid and allowed
     */
    async authorizedData (authorization = {}, data = {}) {

        this.debug('authorizedData');

        try {

            let objectPassed = false;
            if (typeof data !== 'object') {

                throw new Error('data must be an object or and array of objects');

            }

            if (!Array.isArray(data)) {

                objectPassed = true;
                data = [ data ];

            }

            for (let i = 0; i < authorization.conditions.length; i++) {

                data = this.rules.run({ rules: authorization.conditions[i], data });

            }

            if (objectPassed === true) {

                if (data.length > 0) {

                    data = data[0];

                } else {

                    data = null;

                }

            }

            return data;

        } catch (error) {

            throw this.error.handle({ error, caller: 'authorizedData' });

        }

    }

    /**
     * check if records are allowed to be viewed
     * @param {string} type - data type can be 'db' or 'es'
     * @param {authorization} authorization - a permissions object returned from allowed
     * @returns {Promise<string|object>} returns string when 'db' and object when 'es'
     */
    async authorizedFilter (type = 'db', authorization = {}) {

        this.debug('authorizedFilter');

        try {

            let filter = '';
            for (let i = 0; i < authorization.conditions.length; i++) {

                filter = await this.rules.query({ rules: authorization.conditions[i], type, filter });

            }

            return filter;

        } catch (error) {

            throw this.error.handle({ error, caller: 'authorizedFilter' });

        }

    }

    /**
     * Get permission object for provided role, if role is an array then it must be true for all named roles
     * @param {object=} options - available options
     * @param {string|array} [options.role=''] - role to check
     * @param {string} [options.resource=''] - resource
     * @param {string} [options.action=''] - action
     * @returns {Promise<permission>} returns permission object
     */
    async allowed ({ role = [], resource = '', action = '', context } = {}) {

        this.debug('allowed');

        try {

            if (typeof role === 'string') {

                role = [ role ];

            }

            let grants = [];
            let connector = this.layerize.permissions.connector;
            if (typeof connector === 'function') {

                grants = await connector({ role, resource, action, layerize: this.layerize, schemaName: this.schemaName });

            } else if (this.table !== '') {

                grants = await this._connector({ role, resource, action });

            } else {

                throw new Error('Unable to find a connector for permissions.');

            }

            const accessControl = new AccessControl({ grants });

            return accessControl.can({ role, action, resource, context });

        } catch (error) {

            throw this.error.handle({ error, caller: 'allowed' });

        }

    }

    /**
     * Get access list object for provided role, if role is an array then it must be true for all named roles
     * @param {object=} options - available options
     * @param {string|array} [options.role=''] - role to check
     * @returns {Promise<accessList>} returns access list object
     */
    async accessList ({ role = [] } = {}) {

        this.debug('accessList');

        try {

            if (typeof role === 'string') {

                role = [ role ];

            }

            let grants = [];
            let connector = this.layerize.permissions.connector;
            if (typeof connector === 'function') {

                grants = await connector({ role, keysOnly: true, layerize: this.layerize, schemaName: this.schemaName });

            } else if (this.table !== '') {

                grants = await this._connector({ role, keysOnly: true });

            } else {

                throw new Error('Unable to find a connector for permissions.');

            }

            const accessControl = new AccessControl();

            return accessControl.accessList({ role, grants });

        } catch (error) {

            throw this.error.handle({ error, caller: 'accessList' });

        }

    }

    /**
     * Compares the access needed against the access allowed and returns true or false
     * @param {object=} options - available options
     * @param {object} [options.access=''] - object containing the required access needed
     * @param {accessList} [options.accessList=''] - object which is provided by accessList() method
     * @returns {boolean} returns true or false
     */
    accessAllowed ({ access, accessList }) {

        this.debug('accessAllowed');

        try {

            /**
             * private internal function for layerize
             * @access private
             * @param {object} accessLine - access
             * @returns {boolean} returns true of false
             */
            let processLine = (accessLine) => {

                let allowed;

                if (typeof accessLine.all !== 'undefined' && accessLine.all.length > 0) {

                    // assume it is true to begin with since it is all
                    allowed = true;

                    for (let i = 0; i < accessLine.all.length; i++) {

                        let accessPermission = accessLine.all[i];

                        if (processLine(accessPermission) !== true) {

                            allowed = false;

                        }

                    }

                } else if (typeof accessLine.any !== 'undefined' && accessLine.any.length > 0) {

                    // assume it is false to begin with since it is any
                    allowed = false;

                    for (let i = 0; i < accessLine.any.length; i++) {

                        let accessPermission = accessLine.any[i];

                        if (processLine(accessPermission) === true) {

                            allowed = true;

                        }

                    }

                } else if (typeof accessLine.resource !== 'undefined' && typeof accessLine.action !== 'undefined') {

                    // assume it is false to begin with
                    allowed = false;

                    let accessPermission = accessLine;

                    if (accessList[`${accessPermission.resource}:${accessPermission.action}`] === true) {

                        allowed = true;

                    }

                } else {

                    // assume it is allowed if no permissions are set
                    allowed = true;

                }

                return allowed;

            };

            return processLine(access);

        } catch (error) {

            throw this.error.handle({ error, caller: 'accessAllowed' });

        }

    }

    /**
     * Generate admin permissions
     * @param {object} definitions - a permissions defintition object
     * @returns {Promise<object>} returns permissions object
     */
    async generateAdminPermissions (definitions = this.definitions) {

        this.debug('generateAdminPermissions');

        try {

            let objPermissions = {};
            let permissions = [];

            let sections = Object.keys(definitions);
            for (let i = 0; i < sections.length; i++) {

                let sectionName = sections[i];
                let section = definitions[sectionName];
                objPermissions[sectionName] = {};

                let resources = Object.keys(section.resources);
                for (let x = 0; x < resources.length; x++) {

                    let resourceName = resources[x];
                    let resource = section.resources[resourceName];
                    objPermissions[sectionName][resourceName] = {};

                    let actions = Object.keys(resource.actions);
                    for (let j = 0; j < actions.length; j++) {

                        let actionName = actions[j];
                        let action = resource.actions[actionName];
                        let obj = {
                            properties: [ '*' ],
                            conditions: {},
                            presets: {}
                        };

                        objPermissions[sectionName][resourceName][actionName] = obj;

                        if (action.properties.length === 0) {

                            objPermissions[sectionName][resourceName][actionName].properties = [];

                        }

                        permissions.push({
                            resource: `${sectionName}:${resourceName}`,
                            action: actionName,
                            properties: obj.properties,
                            conditions: obj.conditions,
                            presets: obj.presets
                        });

                    }

                }

            }

            return { map: objPermissions, permissions };

        } catch (error) {

            throw this.error.handle({ error, caller: 'generateAdminPermissions' });

        }

    }

    /**
     * Load permission definitions into the system
     * @param {string} globPattern - a file glob pattern to schema locations
     * @returns {Promise<object>} returns object of loaded defintions
     */
    async loadDefinitions (globPattern = '') {

        this.debug('loadDefinitions');

        try {

            let permissionFiles = glob.sync(globPattern);

            let definitions = {};

            for (let i = 0; i < permissionFiles.length; i++) {

                let definition = JSON.parse(fs.readFileSync(permissionFiles[i], 'utf8'));

                if (typeof definition.section === 'string' && definition.section !== '') {

                    if (typeof definitions[definition.section] === 'undefined') {

                        definitions[definition.section] = {
                            section: definition.section,
                            resources: {}
                        };

                    }

                } else {

                    throw new Error('\'section\' property is required in the permission definition and must be an string.');

                }

                if (typeof definition.resource === 'string' && definition.resource !== '') {

                    let resources = definitions[definition.section].resources;
                    if (typeof resources[definition.resource] !== 'undefined') {

                        throw new Error(`Another resource called '${definition.resource}' already exists for section '${definition.section}'. Please rename and try again.`);

                    }

                    resources[definition.resource] = {
                        resource: definition.resource,
                        table: definition.table || '',
                        actions: {}
                    };

                    let resource = definitions[definition.section].resources[definition.resource];

                    if (typeof definition.actions === 'undefined' || typeof definition.actions !== 'object' || Array.isArray(definition.actions)) {

                        throw new Error('\'actions\' property is required in the permission definition and must be an object.');

                    }

                    let actions = Object.keys(definition.actions);
                    for (let x = 0; x < actions.length; x++) {

                        let actionName = actions[x];
                        let action = definition.actions[actionName];
                        resource.actions[actionName] = {

                            description: String(action.description) || '',
                            properties: await this.__buildPermissionProperties(action.properties, resource),
                            conditionals: []

                        };

                    }

                } else {

                    throw new Error('\'resource\' property is required in the permission definition and must be an string.');

                }

            }

            this.definitions = definitions;

            return this.definitions;

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadDefinitions' });

        }

    }

    /**
     * Sets the role into the default permission cache structure
     * @param {string} role - role to set
     * @param {array} [permissions=[]] - a permissions to set on role
     * @param {boolean} [admin=false] - should role inherit admin
     * @param {boolean} [adminRole='full'] - The base role used when admin is true, 'full' means all permissions are granted with in the application. If this is a sub account the the adminRole should be set to another role name that defines those limitations.
     * @returns {Promise<array>} returns permissions that were set
     */
    async setRole ({ role = '', permissions = [], admin = false, adminRole = 'full' } = {}) {

        this.debug('setRole', role, admin, adminRole);

        try {

            let objHash = {
                settings: {
                    admin,
                    adminRole,
                    hash: this.hash
                }
            };

            if (admin) {

                if (adminRole === 'full') {

                    ({ permissions } = await this.generateAdminPermissions());

                } else {

                    //

                }

            }

            for (let i = 0; i < permissions.length; i++) {

                let permission = permissions[i];

                let aryResource = permission.resource.split(':');

                if (aryResource.length < 2) {

                    throw new Error(`Invalid resource: '${permission.resource}' the resource property must be formated 'sectionName:resourceName'.`);

                }

                let permissionKey = `${permission.resource}:${permission.action}`;
                if (typeof objHash[permissionKey] === 'undefined') {

                    objHash[permissionKey] = extend(true, {
                        properties: [ '*' ],
                        conditions: {},
                        presets: {}
                    }, permission);

                }

            }

            const cacheKey = this.cache.key(this.cacheKey, role);
            await this.cache.hash.set(cacheKey, objHash, true);

            return permissions;

        } catch (error) {

            throw this.error.handle({ error, caller: 'setRole' });

        }

    }

    /**
     * A private internal method for building permission properties.
     * @access private
     * @param {array} [properties=[]] - array of properties to be parsed
     * @param {object} [resource={}] - resource record
     * @returns {Promise<array>} returns full list of properties available
     */
    async __buildPermissionProperties (properties = [], resource = {}) {

        this.debug('__buildPermissionProperties');

        try {

            let props = [];
            for (let i = 0; i < properties.length; i++) {

                let obj = properties[i];

                if (typeof obj.ref !== 'undefined' && resource.table !== '') {

                    const schema = this.layerize.schemas.layerize[resource.table];
                    let schemaProps = Object.keys(schema.properties);

                    switch (obj.ref) {

                        case '*:create':

                            for (let x = 0; x < schemaProps.length; x++) {

                                let columnName = schemaProps[x];
                                let column = schema.properties[columnName];
                                if (typeof column.readOnly === 'undefined' || column.readOnly === false || (column.definableOnCreate && column.readOnly)) {

                                    props.push({
                                        property: columnName
                                    });

                                }

                            }

                            break;

                        case '*:update':

                            for (let x = 0; x < schemaProps.length; x++) {

                                let columnName = schemaProps[x];
                                let column = schema.properties[columnName];
                                if (typeof column.readOnly === 'undefined' || column.readOnly === false) {

                                    props.push({
                                        property: columnName
                                    });

                                }

                            }
                            break;

                        default:

                            for (let x = 0; x < schemaProps.length; x++) {

                                let columnName = schemaProps[x];
                                props.push({
                                    property: columnName
                                });

                            }
                            break;

                    }

                } else {

                    let property = obj.property;

                    switch (property.charAt(0)) {

                        case '!':

                            property = property.substring(1);

                            let nP = [];
                            for (let x = 0; x < props.length; x++) {

                                if (props[x].property !== property) {

                                    nP.push(props[x]);

                                }

                            }

                            props = nP;

                            break;

                        default:

                            props.push(obj);

                    }

                }

            }

            return props;

        } catch (error) {

            throw this.error.handle({ error, caller: '__buildPermissionProperties' });

        }

    }

    /**
     * Use the default internal connector to get permissions
     * @param {object=} options - available options
     * @param {array} [options.role=[]] - role
     * @param {string} [options.action=''] - action
     * @param {string} [options.resource=''] - resource
     * @param {boolean} [options.keysOnly=false] - resource
     * @returns {Promise<object|array>} returns list of grants
     */
    async _connector ({ role = [], action = '', resource = '', keysOnly = false } = {}) {

        this.debug('_connector', role);

        try {

            let aryResource = resource.split(':');

            if (resource !== '' && aryResource.length < 2) {

                throw new Error('\'resource\' property must be formated \'sectionName:resourceName\'.');

            }

            let objGrants = {};

            for (let i = 0; i < role.length; i++) {

                let roleName = role[i];
                let grants = [];

                const cacheKey = this.cache.key(this.cacheKey, roleName);

                if (keysOnly) {

                    grants = await this.cache.hash.keys(cacheKey);

                    if (grants.length === 0) {

                        grants = await this.__connectorSetRole({ role: roleName, keysOnly });

                    }

                    grants = grants.filter(key => key !== 'settings');

                } else {

                    let permissionKeys = ['settings', `${resource}:${action}`];
                    let results = await this.cache.hash.get(cacheKey, permissionKeys);

                    // if the cache could not be found then go set it.
                    if (Array.isArray(results) && results[0] === null) {

                        results = await this.__connectorSetRole({ role: roleName, permissionKeys });

                    } else {

                        let settings = results[0];
                        // check to make sure it is the latest version of permissions, if not repopulate
                        if (settings.hash !== this.hash) {

                            results = await this.__connectorSetRole({ role: roleName, permissionKeys });

                        }

                    }

                    if (results[1] !== null) {

                        grants.push(results[1]);

                    }

                }

                objGrants[roleName] = { grants };

            }

            return objGrants;

        } catch (error) {

            throw this.error.handle({ error, caller: '_connector' });

        }

    }

    /**
     * A private internal method for loading role into memory if it does not exist
     * @access private
     * @param {string} [options.role=''] - role
     * @returns {Promise<object>} returns list of grants
     */
    async __connectorSetRole ({ role = '', permissionKeys = [], keysOnly = false }) {

        this.debug('__connectorSetRole', role);

        try {

            let results = [{}, null];

            let getRole = this.layerize.permissions.getRole;
            if (typeof getRole === 'function') {

                // get role and pass to set role
                await this.setRole(await getRole(role, { layerize: this.layerize, schemaName: this.schemaName }));

            } else {

                throw new Error('getRole connector is not defined for permissions.');

            }

            const cacheKey = this.cache.key(this.cacheKey, role);

            if (keysOnly === true) {

                results = await this.cache.hash.keys(cacheKey);

            } else {

                results = await this.cache.hash.get(cacheKey, permissionKeys);

            }

            return results;

        } catch (error) {

            throw this.error.handle({ error, caller: '__connectorSetRole' });

        }

    }

}

module.exports = Permissions;
