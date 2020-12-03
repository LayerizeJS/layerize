'use strict';
/* eslint-disable new-cap, camelcase, no-warning-comments*/

const defaultDebug = require('debug')('layerize:permissions');
const AccessControl = require('./access-control');
const Rules = require('./rules');
const { errors, helpers } = require('../utils');
const { parseLiteral } = helpers;
const extend = require('extend');

/**
 * The Permissions class is a handling role based permissions.
 */
class Permissions {

    /**
     * Create a Permission.
     * @param {object} config - available config
     */
    constructor ({ layerize, schemaName = 'public', debug = defaultDebug } = {}) {

        if (typeof layerize !== 'object' || layerize === null || typeof layerize === 'undefined') {

            throw new errors.Error({ message: 'A valid instance of Layerize must be passed to the class.' });

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
         * A table name to permission resource map for easier lookups
         * @member {object}
         * */
        this.tableToResourceLookup = this.layerize.permissions.tableToResourceLookup || {};

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
    authorized (permission = {}) {

        this.debug('authorized');

        try {

            let obj = {
                conditions: [],
                properties: []
            };

            if (Object.keys(permission).length > 0) {

                if (permission.granted === false) {

                    throw new errors.Error({ message: `You do not have permission to execute '${permission.action}' under resource '${permission.resource}'.` });

                }

                for (let i = 0; i < permission._.length; i++) {

                    let _ = permission._[i];
                    if (typeof _.condition !== 'undefined' && Object.keys(_.condition).length > 0) {

                        obj.conditions.push(_.condition);

                    }

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
     * check if records are allowed based on conditions, it will remove records that are not allowed.
     * @param {authorization} authorization - a permissions object returned from allowed
     * @param {object|array} data - an object or array of objects to check for authorization
     * @returns {Promise<object|array>} returns object or array of data that is allowed
     * @returns {Promise<null>} returns null if object is passed and not allowed
     */
    authorizedData (authorization = {}, data = {}) {

        this.debug('authorizedData');

        try {

            let objectPassed = false;
            if (typeof data !== 'object') {

                throw new errors.Error({ message: 'data must be an object or and array of objects' });

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
     * check if records properties are allowed based on conditions, it will remove top level properties and replace child level properties that are not allowed.
     * @param {authorization} authorization - a permissions object returned from allowed
     * @param {object|array} data - an object or array of objects to check for authorization
     * @param {object|array} currentRecord - an object or array of objects to check for authorization
     * @returns {Promise<object|array>} returns object or array of data with properties that are allowed and/or replaced
     */
    authorizedProperties (authorization = {}, data = {}, currentRecord = {}) {

        this.debug('authorizedProperties');

        try {

            let objectPassed = false;
            if (typeof data !== 'object') {

                throw new errors.Error({ message: 'authorizedProperties() arguments, data, must be an object or and array of objects.' });

            }

            if (typeof currentRecord !== 'object') {

                throw new errors.Error({ message: 'authorizedProperties() arguments, currentRecord, must be an object or and array of objects.' });

            }

            if (Array.isArray(data)) {

                if (!Array.isArray(currentRecord)) {

                    throw new errors.Error({ message: 'authorizedProperties() arguments, data and currentRecord, must be the same type... an object or an array.' });

                }

            } else {

                if (Array.isArray(currentRecord)) {

                    throw new errors.Error({ message: 'authorizedProperties() arguments, data and currentRecord, must be the same type... an object or an array.' });

                }

                objectPassed = true;
                data = [ data ];
                currentRecord = [ currentRecord ];

            }

            for (let i = 0; i < data.length; i++) {

                // making sure no changes happen to our original currentRecord object
                let copyCurrentRecord = extend(true, {}, currentRecord[i]);

                // get cleaned data object
                data[i] = this.__processProperties(authorization.properties, data[i], copyCurrentRecord, true);

            }

            if (objectPassed === true) {

                data = data[0];

            }

            return data;

        } catch (error) {

            throw this.error.handle({ error, caller: 'authorizedData' });

        }

    }

    /**
     * builds a data layer filter for authorization to be used against the given data layer
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
     * @param {object} options.context - data object the permission is analyzed against
     * @param {object} options.permissionVars - data used to populate dynamic permission variables
     * @returns {Promise<authorization>} returns authorization object
     */
    async allowed ({ role = [], resource = '', action = '', context, permissionVars = {} } = {}) {

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

                throw new errors.Error({ message: 'Unable to find a connector for permissions.' });

            }

            grants = this.__injectPermissionVars({ grants, permissionVars });

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
     * @param {object} options.permissionVars - data used to populate dynamic permission variables
     * @returns {Promise<accessList>} returns access list object
     */
    async accessList ({ role = [], permissionVars } = {}) {

        this.debug('accessList');

        try {

            if (typeof role === 'string') {

                role = [ role ];

            }

            let grants = [];
            let connector = this.layerize.permissions.connector;
            if (typeof connector === 'function') {

                grants = await connector({ role, keysOnly: true, layerize: this.layerize, schemaName: this.schemaName, permissionVars });

            } else if (this.table !== '') {

                grants = await this._connector({ role, keysOnly: true, permissionVars });

            } else {

                throw new errors.Error({ message: 'Unable to find a connector for permissions.' });

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

        this.debug('access', access);

        this.debug('accessList', accessList);

        try {

            /**
             * private internal function for layerize
             * @access private
             * @param {object} accessLine - access
             * @returns {boolean} returns true of false
             */
            let processLine = (accessLine) => {

                let allowed = false;

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
                    // allowed = true;

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
    generateAdminPermissions (definitions = this.definitions) {

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
                            condition: {},
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
                            condition: obj.condition,
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
     * @param {array} permissionFiles - a file glob pattern to schema locations
     * @returns {Promise<object>} returns object of loaded definitions
     */
    loadDefinitions (permissionFiles = []) {

        this.debug('loadDefinitions');

        try {

            const definitions = {};
            const tableToResourceLookup = {};

            for (let i = 0; i < permissionFiles.length; i++) {

                let definition = permissionFiles[i];

                if (typeof definition.section === 'string' && definition.section !== '') {

                    if (typeof definitions[definition.section] === 'undefined') {

                        definitions[definition.section] = {
                            section: definition.section,
                            resources: {}
                        };

                    }

                } else {

                    throw new errors.Error({ message: '\'section\' property is required in the permission definition and must be an string.' });

                }

                if (typeof definition.resource === 'string' && definition.resource !== '') {

                    let resources = definitions[definition.section].resources;
                    if (typeof resources[definition.resource] !== 'undefined') {

                        throw new errors.Error({ message: `Another resource called '${definition.resource}' already exists for section '${definition.section}'. Please rename and try again.` });

                    }

                    resources[definition.resource] = {
                        resource: definition.resource,
                        table: definition.table || '',
                        actions: {}
                    };

                    if (typeof definition.table !== 'undefined' && definition.table !== '') {

                        tableToResourceLookup[definition.table] = `${definition.section}:${definition.resource}`;

                    }

                    let resource = definitions[definition.section].resources[definition.resource];

                    if (typeof definition.actions === 'undefined' || typeof definition.actions !== 'object' || Array.isArray(definition.actions)) {

                        throw new errors.Error({ message: '\'actions\' property is required in the permission definition and must be an object.' });

                    }

                    let actions = Object.keys(definition.actions);
                    for (let x = 0; x < actions.length; x++) {

                        let actionName = actions[x];
                        let action = definition.actions[actionName];
                        let objAction = {

                            description: String(action.description || ''),
                            properties: this.__buildPermissionProperties(action.properties, resource),
                            conditionals: this.__buildPermissionProperties(action.conditionals, resource),
                            presets: this.__buildPermissionProperties(action.presets, resource),
                            restrictions: {
                                presets: false,
                                properties: false,
                                conditionals: false
                            }

                        };

                        if (objAction.properties.length > 0) {

                            objAction.restrictions.properties = true;

                        }

                        if (objAction.conditionals.length > 0) {

                            objAction.restrictions.conditionals = true;

                        }

                        if (objAction.presets.length > 0) {

                            objAction.restrictions.presets = true;

                        }

                        resource.actions[actionName] = objAction;

                    }

                } else {

                    throw new errors.Error({ message: '\'resource\' property is required in the permission definition and must be an string.' });

                }

            }

            this.definitions = definitions;
            this.tableToResourceLookup = tableToResourceLookup;

            return {
                definitions: this.definitions,
                tableToResourceLookup: this.tableToResourceLookup
            };

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadDefinitions' });

        }

    }

    /**
     * Get formatted definitions
     * @param {object} definitions - a permissions defintition object
     * @returns {object} returns object
     */
    getFormattedDefinitions (definitions = this.definitions) {

        this.debug('getFormattedDefinitions');

        try {

            let obj = {
                permissions: []
            };

            let sectionKeys = Object.keys(definitions).sort();
            for (let i = 0; i < sectionKeys.length; i++) {

                let objSection = definitions[sectionKeys[i]];

                let resourceKeys = Object.keys(objSection.resources).sort();
                for (let x = 0; x < resourceKeys.length; x++) {

                    let objResource = objSection.resources[resourceKeys[x]];

                    let pResource = {
                        section: objSection.section,
                        resource: objResource.resource,
                        table: objResource.table,
                        actions: []
                    };

                    let actionKeys = Object.keys(objResource.actions).sort();
                    for (let j = 0; j < actionKeys.length; j++) {

                        let actionKey = actionKeys[j];
                        let action = objResource.actions[actionKey];

                        let objAction = {
                            action: actionKey,
                            properties: this.__buildPermissionProperties(action.properties, objResource),
                            conditionals: this.__buildPermissionProperties(action.conditionals, objResource),
                            presets: this.__buildPermissionProperties(action.presets, objResource),
                            restrictions: {
                                presets: false,
                                properties: false,
                                conditionals: false
                            }
                        };

                        if (objAction.properties.length > 0) {

                            objAction.restrictions.properties = true;

                        }

                        if (objAction.conditionals.length > 0) {

                            objAction.restrictions.conditionals = true;

                        }

                        if (objAction.presets.length > 0) {

                            objAction.restrictions.presets = true;

                        }

                        pResource.actions.push(objAction);

                    }

                    obj.permissions.push(pResource);

                }

            }

            return obj;

        } catch (error) {

            throw this.error.handle({ error, caller: 'getFormattedDefinitions' });

        }

    }

    /**
     * Get restrictable properties
     * @param {string} section - name of the permission section
     * @param {string} resource - name of the permission resource
     * @param {string} action - name of the permission action
     * @returns {object} returns object
     */
    getRestrictableProperties ({ section = '', resource = '', action = '' } = {}) {

        this.debug('getRestrictableProperties');

        try {

            let properties = [];

            if (typeof this.definitions[section] === 'undefined') {

                throw new errors.Error({ message: `Unable to find section: '${section}' in permissions definitions.` });

            }

            if (typeof this.definitions[section].resources[resource] === 'undefined') {

                throw new errors.Error({ message: `Unable to find resource '${resource}' in section: '${section}' of the permissions definitions.` });

            }

            if (typeof this.definitions[section].resources[resource].actions[action] === 'undefined') {

                throw new errors.Error({ message: `Unable to find action '${action}' in resource '${section}:${resource}' of the permissions definitions.` });

            }

            let objResource = this.definitions[section].resources[resource];
            let objAction = objResource.actions[action];

            properties = this.__buildPermissionProperties(objAction.properties || [], objResource);

            return properties;

        } catch (error) {

            throw this.error.handle({ error, caller: 'getRestrictableProperties' });

        }

    }

    /**
     * Remove the roles into the default permission cache structure
     * @param {array} roles - role to remove
     * @returns {Promise<success>} returns success
     */
    async removeRoles ({ roles = [] } = {}) {

        this.debug('removeRoles', roles);

        try {

            for (let i = 0; i < roles.length; i++) {

                let cacheKey = this.cache.key(this.cacheKey, roles[i]);
                await this.cache.clear(cacheKey);

            }

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'removeRoles' });

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

                    // TODO: Admin Role should be limited to parent role

                }

            }

            for (let i = 0; i < permissions.length; i++) {

                let permission = permissions[i];

                let aryResource = permission.resource.split(':');

                if (aryResource.length < 2) {

                    throw new errors.Error({ message: `Invalid resource: '${permission.resource}' the resource property must be formated 'sectionName:resourceName'.` });

                }

                let permissionKey = `${permission.resource}:${permission.action}`;
                if (typeof objHash[permissionKey] === 'undefined') {

                    objHash[permissionKey] = extend(true, {
                        properties: [ '*' ],
                        condition: {},
                        presets: {}
                    }, permission);

                }

            }

            const cacheKey = this.cache.key(this.cacheKey, role);
            await this.cache.clear(cacheKey);
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
    __buildPermissionProperties (properties = [], resource = {}) {

        try {

            /**
             * A private internal sub method for getting the defined properties.
             * @access private
             * @param {object} [properties={}] - properties
             * @param {object} [parent={}] - properties
             * @param {string} [type='*'] - type
             * @returns {Promise<array>} returns full list of properties available
             */
            let getProperties = ({ properties = {}, parent = '', type = '*' } = {}) => {

                let schemaProps = Object.keys(properties);
                let props = [];
                for (let x = 0; x < schemaProps.length; x++) {

                    let columnName = schemaProps[x];
                    let column = properties[columnName];

                    if (
                        (type === '*:create' && (typeof column.readOnly === 'undefined' || column.readOnly === false || (column.definableOnCreate && column.readOnly))) ||
                        (type === '*') ||
                        (type === '*:update' && (typeof column.readOnly === 'undefined' || column.readOnly === false))
                    ) {

                        if (parent !== '') {

                            columnName = `${parent}.${columnName}`;

                        }

                        let objProp = {
                            type: column.type,
                            property: columnName
                        };

                        if (typeof objProp.type === 'undefined' && column.format === 'date-time') {

                            objProp.type = 'date';

                        }

                        if (typeof column.properties !== 'undefined') {

                            objProp.properties = getProperties({ properties: column.properties, parent: columnName });

                        } else if (typeof column.items !== 'undefined' && typeof column.items.properties !== 'undefined') {

                            objProp.properties = getProperties({ properties: column.items.properties, parent: columnName });

                        }

                        props.push(objProp);

                    }

                }
                return props;

            };

            let props = [];
            for (let i = 0; i < properties.length; i++) {

                let obj = properties[i];

                if (typeof obj.ref !== 'undefined' && resource.table !== '') {

                    const schema = this.layerize.schemas.layerize[resource.table];

                    if (typeof schema === 'undefined') {

                        throw new errors.Error({ message: `Can not find schema '${resource.table}'.` });

                    }

                    switch (obj.ref) {

                        case '*:create':

                            props = props.concat(getProperties({ properties: schema.properties, type: '*:create' }));

                            break;

                        case '*:update':

                            props = props.concat(getProperties({ properties: schema.properties, type: '*:update' }));
                            break;

                        default:

                            props = props.concat(getProperties({ properties: schema.properties, type: '*' }));
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

        this.debug('_connector', role, resource);

        try {

            let aryResource = resource.split(':');

            if (resource !== '' && aryResource.length < 2) {

                throw new errors.Error({ message: '\'resource\' property must be formated \'sectionName:resourceName\'.' });

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

                throw new errors.Error({ message: 'getRole connector is not defined for permissions.' });

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

    /**
     * A private internal method for processing properties
     * @access private
     * @param {object} properties - property
     * @param {object} data - an object to check for authorization
     * @param {object} currentRecord - an object to check for authorization
     * @param {boolean} topLevel - is this a top level property
     * @returns {Promise<object>} returns list of grants
     */
    __processProperties (properties, data = {}, currentRecord = {}, topLevel = false) {

        let props = [];
        for (let x = 0; x < properties.length; x++) {

            if (Array.isArray(properties[x])) {

                props = props.concat(properties[x]);

            } else {

                props.push(properties[x]);

            }

        }

        for (let i = 0; i < props.length; i++) {

            let property = props[i];
            let propertyKey = property.property;

            if (!topLevel) {

                // grab last property name from string as that is the property Id
                propertyKey = String(property.property).split('.').pop();

            }

            if (typeof data[propertyKey] !== 'undefined') {

                let propertyAllowed = true;

                switch (property.type) {

                    case 'never':
                        propertyAllowed = true;
                        break;

                    case 'always':
                        propertyAllowed = false;
                        break;

                    default: // 'condition'

                        /**
                         * We will only analyze rules against the currentRecord, not against the new updates, as that is the current state of the record is most important.
                         */

                        // if rule is true the property is not allowed
                        propertyAllowed = !this.rules._processRule({ rules: property.condition, data: currentRecord });

                }

                if (!propertyAllowed) {

                    if (topLevel) {

                        delete data[propertyKey];

                    } else {

                        data[propertyKey] = currentRecord[propertyKey];

                    }

                } else {

                    if (property.properties.length > 0) {

                        data[propertyKey] = this.__processProperties(property.properties, data[propertyKey], currentRecord[propertyKey], false);

                    }

                }

            }

        }

        return data;

    }

    /**
     * Inject permissions variables into Process rules
     * @param {object=} options - available options
     * @param {object=} [options.grants={}] - rules object
     * @returns {grants} returns sql where statement
     */
    __injectPermissionVars ({ grants = {}, permissionVars = {} } = {}) {

        this.debug('__injectPermissionVars');
        try {

            const grantRoles = Object.keys(grants);

            for (let x = 0; x < grantRoles.length; x++) {

                const grantRole = grants[grantRoles[x]];

                for (let i = 0; i < grantRole.grants.length; i++) {

                    const grant = grantRole.grants[i];

                    grant.condition = this.__injectPermissionVarRules({ rules: grant.condition, permissionVars });

                }

            }

            return grants;

        } catch (error) {

            throw this.error.handle({ error, caller: '__injectPermissionVars' });

        }

    }

    /**
     * Inject permissions variables into Process rules
     * @param {object=} options - available options
     * @param {object=} [options.rules={}] - rules object
     * @returns {rules} returns sql where statement
     */
    __injectPermissionVarRules ({ rules = {}, permissionVars = {} } = {}) {

        this.debug('__injectPermissionVarRules');
        try {

            switch (rules.type) {

                case 'all':
                case 'any':
                    for (let i = 0; i < rules.children.length; i++) {

                        rules.children[i] = this.__injectPermissionVarRules({ rules: rules.children[i], permissionVars });

                    }

                    break;

                case 'fact':

                    rules.value = parseLiteral(rules.value || '', permissionVars, 'user');

                    break;

            }

            return rules;

        } catch (error) {

            throw this.error.handle({ error, caller: '__injectPermissionVarRules' });

        }

    }

}

module.exports = Permissions;
