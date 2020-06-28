'use strict';

const debug = require('debug')('layerize:schemas');
const glob = require('glob');
const fs = require('fs');
const extend = require('extend');
const { errors, layerizeSchema, objects } = require('../utils');
const { parseRefs } = layerizeSchema;
const pjson = require('../../package.json');

/**
 * The SchemaBuilder class loads provided json schema files into Layerize format.
 */
class SchemaBuilder {

    /**
     * Create a SchemaBuilder.
     * @param {object} config - available config
     * @param {JsonValidator} config.validator - an instance of a JsonValidator
     */
    constructor ({ validator = {} } = {}) {

        this.debug = debug;
        this.error = errors;
        this.validator = validator;

    }

    /**
     * loads provided json schema files into Layerize format.
     * @param {string} schemaPattern - a file pattern to schema locations
     * @param {boolean} [parseWithLayerize=true] - if true the returned schema is also parsed by layerized
     * @returns {Promise<object>} elasticsearch body object
     */
    async load (schemaPattern = '', { parseWithLayerize = true } = {}) {

        this.debug('load()', schemaPattern);
        try {

            let objSchemas = {
                version: pjson.version,
                schemas: []
            };

            let schemaFiles = glob.sync(schemaPattern);
            let arySchemas = [];

            this.debug(` -- Found ${schemaFiles.length} schema files.`);

            for (let i = 0; i < schemaFiles.length; i++) {

                let schema = JSON.parse(fs.readFileSync(schemaFiles[i], 'utf8'));
                if (schemaFiles[i].indexOf('_config.json') > -1) {

                    /**
                     * Remove properties that can not be overwritten later
                     */
                    delete schema.hash;
                    delete schema.raw;
                    delete schema.schemas;
                    delete schema.layerize;

                    objSchemas = extend(true, objSchemas, schema);

                } else {

                    schema.$async = true;
                    arySchemas.push(schema);

                    if (this.validator.validateSchema(schema) === false) {

                        this.debug(this.validator.errors);

                    }

                }

            }

            if (arySchemas.length > 0) {

                this.validator.addSchema(arySchemas);
                this.debug(` -- Loaded ${arySchemas.length} schema files.`);

                for (let i = 0; i < arySchemas.length; i++) {

                    let objSchema = arySchemas[i];

                    let id = objSchema.$id || '';
                    let schema = parseRefs({ jsonSchema: this.validator.getSchema(id) });

                    if (schema !== null) {

                        objSchemas.schemas.push(schema);

                    }

                }

            }

            if (parseWithLayerize === true) {

                objSchemas = await this.layerize({ schemas: objSchemas });

            }

            return objSchemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'load' });

        }

    }

    /**
     * loads provided json schema files into Layerize format.
     * @param {object} layerize - a file pattern to schema locations
     * @returns {Promise<object>} elasticsearch body object
     */
    async layerize ({ schemas = {} } = {}) {

        this.debug('layerize()');
        try {

            /**
             * private internal function for layerize
             * @access private
             * @param {string} schema - schema
             * @param {object} details - details
             * @returns {object} returns object
             */
            let recursive = (schema, { properties = {}, dynamicDefaults = {}, required = [], elasticSearch } = {}) => {

                if (typeof schema.allOf !== 'undefined' && typeof schema.properties !== 'undefined') {

                    throw new Error('Property name \'properties\' must reside within property \'allOf\' when being used together.');

                }

                if (typeof schema.allOf !== 'undefined') {

                    for (let x = 0; x < schema.allOf.length; x++) {

                        ({ properties, dynamicDefaults, required, elasticSearch } = recursive(schema.allOf[x], { properties, dynamicDefaults, required, elasticSearch }));

                    }

                    delete schema.allOf;

                } else {

                    if (typeof schema.dynamicDefaults !== 'undefined') {

                        dynamicDefaults = extend(true, dynamicDefaults, schema.dynamicDefaults);

                    }

                    if (typeof schema.required !== 'undefined' && schema.required.length > 0) {

                        required = required.concat(schema.required);

                    }

                    if (typeof schema.elasticSearch !== 'undefined') {

                        elasticSearch = extend(true, elasticSearch, schema.elasticSearch);

                    }

                    let props = Object.keys(schema.properties || {});
                    for (let x = 0; x < props.length; x++) {

                        properties[props[x]] = schema.properties[props[x]];

                        let property = properties[props[x]];

                        if (typeof property.allOf !== 'undefined') {

                            for (let i = 0; i < property.allOf.length; i++) {

                                ({ properties: property.properties, dynamicDefaults: property.dynamicDefaults, required: property.required, elasticSearch: property.elasticSearch } = recursive(property.allOf[i], { properties: property.properties, dynamicDefaults: property.dynamicDefaults, required: property.required, elasticSearch: property.elasticSearch }));

                            }

                            delete property.allOf;

                        }

                    }

                }

                if (schema.$root) {

                    if (typeof schema.dynamicDefaults !== 'undefined') {

                        dynamicDefaults = extend(true, dynamicDefaults, schema.dynamicDefaults);

                    }

                    if (typeof schema.elasticSearch !== 'undefined') {

                        elasticSearch = extend(true, elasticSearch, schema.elasticSearch);

                    }

                }

                return { properties, dynamicDefaults, required, elasticSearch };

            };

            schemas.hash = '';
            schemas.layerize = {};
            schemas.includes = {};
            schemas.raw = schemas.schemas;

            delete schemas.schemas;

            schemas.raw.sort(objects.sortBy({ name: '$id' }));
            for (let i = 0; i < schemas.raw.length; i++) {

                let schema = schemas.raw[i];
                if (typeof schema.i18n === 'undefined') {

                    schema.i18n = {};

                }

                let properties = {};
                let dynamicDefaults = {};
                let elasticSearch = {};
                let required = [];

                ({ properties, dynamicDefaults, required, elasticSearch } = recursive(schema, { properties, dynamicDefaults, required, elasticSearch }));

                let table = {
                    name: schema.$id,
                    version: schemas.version,
                    primaryKey: '',
                    timestamps: [],
                    includes: {},
                    columns: {},
                    esIndex: {},
                    esEnabled: false,
                    dbEnabled: true,
                    cacheEnabled: false,
                    dynamicDefaults,
                    properties,
                    required,
                    indexes: [],
                    i18n: {
                        singular: schema.i18n.singular || '',
                        plural: schema.i18n.plural || ''
                    },
                    permissions: schema.permissions,
                    baseObject: {}
                };

                let esProperties = {};

                if (typeof schemas.includes[table.name] === 'undefined') {

                    schemas.includes[table.name] = {
                        referenced: {},
                        source: {}
                    };

                }

                let propertyNames = Object.keys(properties);
                for (let x = 0; x < propertyNames.length; x++) {

                    let propertyName = propertyNames[x];
                    let property = properties[propertyName];
                    let column = {
                        name: propertyName,
                        type: property.type,
                        serial: Boolean(property.serial),
                        nullable: (typeof property.nullable === 'undefined') ? true : Boolean(property.nullable),
                        primary: Boolean(property.primaryKey),
                        unique: Boolean(property.unique)
                    };
                    let esType = 'keyword';

                    if (table.primaryKey === '' && column.primary) {

                        table.primaryKey = column.name;

                    }

                    if (column.serial === false) {

                        delete column.serial;

                    } else {

                        column.nullable = false;

                    }

                    if (column.primary === false) {

                        delete column.primary;

                    } else {

                        column.nullable = false;

                    }

                    if (column.unique === false || column.primary === true) {

                        delete column.unique;

                    }

                    if (typeof property.default !== 'undefined') {

                        column.default = property.default;

                    }

                    if (typeof property.database !== 'undefined' && typeof property.database.default !== 'undefined') {

                        column.default = property.database.default;

                    }

                    if (typeof property.foreign !== 'undefined') {

                        column.foreign = property.foreign;

                        if (typeof property.foreign.reference === 'undefined') {

                            throw new Error(`Property '${propertyName}' within table '${table.name}' is missing the 'reference' property inside 'foriegn' declaration.`);

                        }

                        if (typeof property.foreign.reference.column === 'undefined') {

                            throw new Error(`Property '${propertyName}' within table '${table.name}' is missing the 'column' property inside 'foriegn.reference' declaration.`);

                        }

                        if (typeof property.foreign.reference.table === 'undefined') {

                            throw new Error(`Property '${propertyName}' within table '${table.name}' is missing the 'table' property inside 'foriegn.reference' declaration.`);

                        }

                        if (typeof property.foreign.reference.relationship === 'undefined') {

                            throw new Error(`Property '${propertyName}' within table '${table.name}' is missing the 'relationship' property inside 'foriegn.reference' declaration.`);

                        }

                        let includeId = property.foreign.reference.table;
                        if (typeof property.foreign.reference.includeId !== 'undefined' && property.foreign.reference.includeId !== '') {

                            includeId = property.foreign.reference.includeId;

                        }

                        let sourceIncludeId = table.name;
                        if (typeof property.foreign.source !== 'undefined' && typeof property.foreign.source.includeId !== 'undefined' && property.foreign.source.includeId !== '') {

                            sourceIncludeId = property.foreign.source.includeId;

                        }

                        if (typeof table.includes[includeId] !== 'undefined') {

                            throw new Error(`Property '${propertyName}' within table '${table.name}' needs to have a unique 'includeId', it is already being used by '${table.includes[includeId].table} -> ${table.includes[includeId].column}'.`);

                        }

                        column.foreign.reference = {
                            propertyTable: table.name,
                            property: propertyName,
                            table: property.foreign.reference.table,
                            column: property.foreign.reference.column,
                            friendlyColumn: property.foreign.reference.friendlyColumn,
                            relationship: property.foreign.reference.relationship,
                            includeId
                        };

                        if (typeof schemas.includes[property.foreign.reference.table] === 'undefined') {

                            schemas.includes[property.foreign.reference.table] = {
                                referenced: {},
                                source: {}
                            };

                        }

                        schemas.includes[table.name].source[includeId] = column.foreign.reference;
                        schemas.includes[property.foreign.reference.table].referenced[sourceIncludeId] = column.foreign.reference;

                        table.includes[includeId] = column.foreign.reference;

                    }

                    if (typeof property.scale !== 'undefined') {

                        column.scale = property.scale;

                    }

                    if (typeof property.readOnly !== 'undefined') {

                        column.readOnly = property.readOnly;

                    }

                    if (typeof property.definableOnCreate !== 'undefined') {

                        column.definableOnCreate = property.definableOnCreate;

                    }

                    if (typeof property.precision !== 'undefined') {

                        column.precision = property.precision;

                    }

                    switch (property.type) {

                        case 'array':

                            esType = 'object';
                            column.type = 'jsonb';

                            if (typeof property.default === 'undefined') {

                                column.default = [];

                            }

                            break;

                        case 'object':

                            esType = 'object';
                            column.type = 'jsonb';

                            if (typeof property.default === 'undefined') {

                                column.default = {};

                            }

                            break;

                        case 'boolean':

                            esType = 'boolean';
                            column.type = 'boolean';
                            break;

                        case 'integer':

                            esType = 'double';
                            if (property.serial) {

                                column.type = 'increments';

                            } else {

                                column.type = 'integer';

                            }
                            break;

                        default:

                            if (typeof property.maxLength !== 'undefined') {

                                column.length = property.maxLength;

                            }

                            switch (property.format) {

                                case 'date-time':

                                    if (property.timestamp) {

                                        column.type = 'timestamp';

                                    } else {

                                        column.type = 'dateTime';

                                    }

                                    esType = 'date';

                                    break;

                                case 'uuid':

                                    column.type = 'string';
                                    column.length = 36;
                                    esType = 'keyword';
                                    break;

                                case 'text':

                                    column.type = 'text';
                                    esType = 'text';
                                    break;

                                default:

                                    esType = 'keyword';
                                    column.type = 'string';

                            }
                            break;

                    }

                    /**
                     * table MUST enable elasticSeach for any ES properties to be recongized
                     */
                    if (elasticSearch.enabled) {

                        /**
                         * only add proeprty if elasticsearch is enabled, which 'undefined' equal to enabled:true
                         */
                        if (typeof property.elasticSearch === 'undefined' || (typeof property.elasticSearch !== 'undefined' && (typeof property.elasticSearch.enabled === 'undefined' || property.elasticSearch.enabled === true))) {

                            table.esEnabled = true;

                            esProperties[propertyName] = {
                                type: esType
                            };

                            if (typeof property.default !== 'undefined') {

                                esProperties[propertyName].null_value = property.default;

                            }

                            if (property.properties) {

                                esProperties[propertyName].properties = this.__esDeepPropertyTranslate(property.properties);

                            }

                            if (typeof property.elasticSearch !== 'undefined') {

                                esProperties[propertyName] = extend(true, esProperties[propertyName], property.elasticSearch);

                            }

                        }

                    }

                    column.hash = objects.hash(column);
                    table.columns[column.name] = column;

                    if (column.type === 'timestamp') {

                        table.timestamps.push({ name: column.name });

                    }

                }

                if (elasticSearch.enabled) {

                    table.esIndex = extend(true, {}, elasticSearch);
                    delete table.esIndex.enabled;
                    table.esIndex.properties = esProperties;

                }

                // validate required
                for (let x = 0; x < table.required.length; x++) {

                    if (typeof table.columns[table.required[x]] === 'undefined') {

                        throw new Error(`Property '${table.required[x]}' is required but is not defined in '${table.name}'.`);

                    }

                }

                this.debug(` -- Load layerize table schema '${table.name}'`);

                table.baseObject = this.__generateBaseObject(table);

                table.hash = objects.hash(table);
                schemas.layerize[table.name] = table;

            }

            /**
             * create hash of all hashes to quickly know if something has changed
             */
            let hashes = Object.keys(schemas.layerize).map(tableName => schemas.layerize[tableName].hash);
            hashes.unshift(schemas.version);
            schemas.hash = objects.hash(hashes);

            return schemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'layerize' });

        }

    }

    /**
     * translate JSON schema property types to ElasticSearch.
     * @param {object} table - a file pattern to schema locations
     * @returns {object} elasticsearch body object
     */
    __generateBaseObject (table) {

        this.debug('__generateBaseObject()');

        try {

            /**
             * recurvise
             * @param {object} properties - a file pattern to schema locations
             * @returns {object} elasticsearch body object
             */
            const recursive = (properties) => {

                const recursiveBaseObject = {};
                const propertyNames = Object.keys(properties);
                for (let i = 0 ; i < propertyNames.length; i++) {

                    const propertyName = propertyNames[i];
                    const property = properties[propertyName];

                    if (typeof property.readOnly === 'undefined' || property.readOnly === false) {

                        switch (property.type) {

                            case 'string':
                                recursiveBaseObject[propertyName] = property.default || property.nullable ? null : '';
                                break;

                            case 'integer':
                                recursiveBaseObject[propertyName] = property.default || property.nullable ? null : 0;
                                break;

                            case 'boolean':
                                recursiveBaseObject[propertyName] = property.default || property.nullable ? null : false;
                                break;

                            case 'array':
                                recursiveBaseObject[propertyName] = property.default || property.nullable ? null : [];
                                break;

                            case 'object':

                                if (typeof property.properties !== 'undefined') {

                                    recursiveBaseObject[propertyName] = recursive(property.properties);

                                } else {

                                    recursiveBaseObject[propertyName] = property.default || property.nullable ? null : {};

                                }
                                break;

                        }

                    }

                }

                return recursiveBaseObject;

            };

            return recursive(table.properties);

        } catch (error) {

            throw this.error.handle({ error, caller: '__generateBaseObject' });

        }

    }

    /**
     * translate JSON schema property types to ElasticSearch.
     * @param {object} properties - a file pattern to schema locations
     * @returns {object} elasticsearch body object
     */
    __esDeepPropertyTranslate ({ properties = {} } = {}) {

        this.debug('__esDeepPropertyTranslate()');
        try {

            properties = extend(true, {}, properties);

            let propertyNames = Object.keys(properties);
            for (let x = 0; x < propertyNames.length; x++) {

                let property = properties[propertyNames[x]];

                switch (property.type) {

                    case 'array':

                        property.type = 'object';
                        break;

                    case 'integer':

                        property.type = 'double';
                        break;

                    case 'object':
                    case 'boolean':

                        // does not change as it is the same

                        break;

                    default:

                        switch (property.format) {

                            case 'date-time':

                                property.type = 'date';
                                break;

                            case 'uuid':

                                property.type = 'keyword';
                                break;

                            case 'text':

                                property.type = 'text';
                                break;

                            default:

                                property.type = 'keyword';
                                break;

                        }
                        break;

                }

            }

            return properties;

        } catch (error) {

            throw this.error.handle({ error, caller: '__esDeepPropertyTranslate' });

        }

    }

}

module.exports = SchemaBuilder;
