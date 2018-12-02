'use strict';

const debug = require('debug')('layerize:schemas');
const glob = require('glob');
const fs = require('fs');
const extend = require('extend');
const { errors, layerizeSchema, objects } = require('../utils');
const { parseRefs } = layerizeSchema;
const pjson = require('../../package.json');

class SchemaBuilder {

    constructor ({ validator = {} } = {}) {

        this.debug = debug;
        this.error = errors;
        this.validator = validator;

    }

    async load (schemaPattern = '') {

        this.debug('load()');
        try {

            let schemas = {
                version: pjson.version,
                hash: '',
                raw: [],
                layerize: {}
            };

            let schemaFiles = glob.sync(schemaPattern);
            let arySchemas = [];

            this.debug(` -- Found ${schemaFiles.length} schema files.`);

            for (let i = 0; i < schemaFiles.length; i++) {

                let schema = JSON.parse(fs.readFileSync(schemaFiles[i], 'utf8'));
                if (schemaFiles[i].indexOf('_layerize.json') > -1) {

                    /**
                     * Remove properties that can not be overwritten
                     */
                    delete schema.hash;
                    delete schema.raw;
                    delete schema.layerize;

                    schemas = extend(true, schemas, schema);

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

                        schemas.raw.push(schema);

                    }

                }

            }

            return await this.layerize({ schemas });

        } catch (error) {

            throw this.error.handle({ error, caller: 'load' });

        }

    }

    async layerize ({ schemas = {} } = {}) {

        this.debug('layerize()');
        try {

            let recursive = (schema, { properties = {}, dynamicDefaults = {}, required = [], elasticSearch } = {}) => {

                if (typeof schema.allOf !== 'undefined' && typeof schema.properties !== 'undefined') {

                    throw new Error('Property name \'properties\' must reside within property \'allOf\' when being used together.');

                }

                if (typeof schema.allOf !== 'undefined') {

                    for (let x = 0; x < schema.allOf.length; x++) {

                        ({ properties, dynamicDefaults, required, elasticSearch } = recursive(schema.allOf[x], { properties, dynamicDefaults, required, elasticSearch }));

                    }

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

            schemas.raw.sort(objects.sortBy({ name: '$id' }));
            for (let i = 0; i < schemas.raw.length; i++) {

                let schema = schemas.raw[i];
                let properties = {};
                let dynamicDefaults = {};
                let elasticSearch = {};
                let required = [];

                ({ properties, dynamicDefaults, required, elasticSearch } = recursive(schema, { properties, dynamicDefaults, required, elasticSearch }));

                // this.debug(schema.$id);
                // this.debug(columns);

                let table = {
                    name: schema.$id,
                    version: schemas.version,
                    primaryKey: '',
                    timestamps: [],
                    columns: {},
                    esIndex: {},
                    esEnabled: false,
                    dbEnabled: true,
                    cacheEnabled: false,
                    dynamicDefaults,
                    properties,
                    required,
                    indexes: []
                };

                let esProperties = {};

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

                    if (column.unique === false) {

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

                                esProperties[propertyName].properties = property.properties;

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

                this.debug(` -- Load layerize table schema '${table.name}'`);

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

}

module.exports = SchemaBuilder;
