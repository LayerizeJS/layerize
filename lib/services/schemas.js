'use strict';

const debug = require('debug')('layerize:schemas');
const glob = require('glob');
const fs = require('fs');
const extend = require('extend');
const { errors, jsonValidator, layerizeSchema, objects } = require('../utils');
const { parseRefs } = layerizeSchema;

class Schemas {

    constructor () {

        this.debug = debug;
        this.errors = errors;

    }

    async load (schemaPattern = '') {

        this.debug('load()');
        try {

            let schemas = {
                raw: [],
                layerize: {}
            };

            let schemaFiles = glob.sync(schemaPattern);
            let arySchemas = [];

            this.debug(` -- Found ${schemaFiles.length} schema files.`);

            for (let i = 0; i < schemaFiles.length; i++) {

                let schema = JSON.parse(fs.readFileSync(schemaFiles[i], 'utf8'));
                schema.$async = true;
                arySchemas.push(schema);

                if (jsonValidator.validateSchema(schema) === false) {

                    this.debug(jsonValidator.errors);

                }

            }

            if (arySchemas.length > 0) {

                jsonValidator.addSchema(arySchemas);
                this.debug(` -- Loaded ${arySchemas.length} schema files.`);

                for (let i = 0; i < arySchemas.length; i++) {

                    let objSchema = arySchemas[i];

                    let id = objSchema.$id || '';
                    let schema = parseRefs({ jsonSchema: jsonValidator.getSchema(id) });

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

            let recursive = (schema, { properties = {}, dynamicDefaults = {}, required = [] } = {}) => {

                if (typeof schema.allOf !== 'undefined' && typeof schema.properties !== 'undefined') {

                    throw new Error('Property name \'properties\' must reside within property \'allOf\' when being used together.');

                }

                if (typeof schema.allOf !== 'undefined') {

                    for (let x = 0; x < schema.allOf.length; x++) {

                        ({ properties, dynamicDefaults, required } = recursive(schema.allOf[x], { properties, dynamicDefaults, required }));

                    }

                } else {

                    if (typeof schema.dynamicDefaults !== 'undefined') {

                        dynamicDefaults = extend(true, dynamicDefaults, schema.dynamicDefaults);

                    }

                    if (typeof schema.required !== 'undefined' && schema.required.length > 0) {

                        required = required.concat(schema.required);

                    }

                    let props = Object.keys(schema.properties || {});
                    for (let x = 0; x < props.length; x++) {

                        properties[props[x]] = schema.properties[props[x]];

                    }

                }

                return { properties, dynamicDefaults, required };

            };

            for (let i = 0; i < schemas.raw.length; i++) {

                let schema = schemas.raw[i];
                let properties = {};
                let dynamicDefaults = {};
                let required = [];

                ({ properties, dynamicDefaults, required } = recursive(schema, { properties, dynamicDefaults, required }));

                // this.debug(schema.$id);
                // this.debug(columns);

                let table = {
                    name: schema.$id,
                    primaryKey: '',
                    columns: {},
                    esIndex: {},
                    dynamicDefaults,
                    properties,
                    required,
                    indexes: []
                };

                // let esProperties = {};

                let propertyNames = Object.keys(properties);
                for (let x = 0; x < propertyNames.length; x++) {

                    let propertyName = propertyNames[x];
                    let property = properties[propertyName];
                    let column = {
                        name: propertyName,
                        type: property.type,
                        serial: Boolean(property.serial),
                        nullable: Boolean(property.nullable),
                        primary: Boolean(property.primaryKey),
                        unique: Boolean(property.unique)
                    };

                    if (table.primaryKey === '' && column.primary) {

                        table.primaryKey = column.name;

                    }

                    if (column.serial === false) {

                        delete column.serial;

                    }

                    if (column.primary === false) {

                        delete column.primary;

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

                    if (typeof property.precision !== 'undefined') {

                        column.precision = property.precision;

                    }

                    switch (property.type) {

                        case 'array':

                            column.type = 'jsonb';

                            if (typeof property.default === 'undefined') {

                                column.default = [];

                            }

                            break;

                        case 'object':

                            column.type = 'jsonb';

                            if (typeof property.default === 'undefined') {

                                column.default = {};

                            }

                            break;

                        case 'boolean':

                            column.type = 'boolean';
                            break;

                        case 'integer':

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

                                    break;

                                case 'uuid':

                                    column.type = 'string';
                                    column.length = 36;
                                    break;

                                case 'text':

                                    column.type = 'text';
                                    break;

                                default:

                                    column.type = 'string';

                            }
                            break;

                    }

                    table.columns[column.name] = column;

                }

                this.debug(` -- Load layerize table schema '${table.name}'`);

                table.hash = objects.hash(table);
                schemas.layerize[table.name] = table;

            }

            return schemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'layerize' });

        }

    }

}

module.exports = Schemas;
