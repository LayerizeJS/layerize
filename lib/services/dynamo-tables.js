'use strict';
/* eslint-disable new-cap, camelcase*/
const debug = require('debug')('layerize:dynamo-tables');
const glob = require('glob');
const fs = require('fs');
const extend = require('extend');
const Joi = require('joi');
const { errors } = require('../utils');
const pjson = require('../../package.json');

/**
 * The DynamoTables class provides a access to schema level dynamo tables.
 */
class DynamoTables {

    /**
     * Create a JsonValidator.
     */
    constructor ({ dynamo }) {

        this.debug = debug;
        this.error = errors;
        this.dynamo = dynamo;
        this.tables = {};
        this.files = {};

    }

    /**
     * get the defined instance of the requested dynamo table
     * @param {string} [name=''] - the type of action being done
     * @param {string} [dbSchemaName=''] - database schema name
     * @returns {dynamo} return object.
     */
    table (name = '', dbSchemaName = '') {

        let tableName = `${dbSchemaName}.${name}`;
        if (typeof this.tables[tableName] === 'undefined') {

            let uniqueSchema = extend(true, {}, this.files.schemas[name]);
            uniqueSchema.tableName = `${dbSchemaName}.${uniqueSchema.tableName}`;
            this.tables[tableName] = this.dynamo.define(tableName, uniqueSchema);

        }

        return this.tables[tableName];

    }

    /**
     * loads provided json dynamo schema files into Layerize.
     * @param {string} schemaPattern - a file pattern to schema locations
     * @returns {Promise<object>} elasticsearch body object
     */
    load (schemaPattern = '') {

        this.debug('load()', schemaPattern);
        try {

            let objSchemas = {
                version: pjson.version,
                schemas: {},
                raw: {}
            };

            let schemaFiles = glob.sync(schemaPattern);

            this.debug(` -- Found ${schemaFiles.length} dynamo schema files.`);

            for (let i = 0; i < schemaFiles.length; i++) {

                let schema = JSON.parse(fs.readFileSync(schemaFiles[i], 'utf8'));
                if (schemaFiles[i].indexOf('_config.json') > -1) {

                    /**
                     * Remove properties that can not be overwritten later
                     */
                    delete schema.hash;
                    delete schema.raw;
                    delete schema.schemas;

                    objSchemas = extend(true, objSchemas, schema);

                } else {

                    let parsedSchema = this.validateSchema(schema, schemaFiles[i]);
                    objSchemas.raw[parsedSchema.tableName] = schema;
                    objSchemas.schemas[parsedSchema.tableName] = parsedSchema;

                }

            }

            this.files = objSchemas;
            return objSchemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'load' });

        }

    }

    /**
     * validates the provided json dynamo schema files into Layerize.
     * @param {object} schema - json dynamo schema
     * @param {string} fileName - fileName of the schema
     * @returns {Promise<object>} dynamo schema
     */
    validateSchema (schema = {}, fileName) {

        let parsedSchema = extend(true, {}, schema);

        // reset schema property as we will be building it.
        parsedSchema.schema = {};

        let keys = Object.keys(schema.schema);
        for (let i = 0; i < keys.length; i++) {

            let key = keys[i];
            let type = key.type || 'string';
            let property = Joi[type]();

            if (typeof key.constraints !== 'undefined') {

                let contraints = Object.keys(key.constraints);
                for (let x = 0; x < contraints.length; x++) {

                    let contraint = contraints[x];
                    property = property[contraint](...key.constraints[contraint]);

                }

            }

            if (key.required) {

                property = property.required();

            }

            parsedSchema.schema[key] = property;

        }

        if (typeof schema.tableName === 'undefined') {

            throw new Error(`Property 'tableName' is required in layerize and missing from dynamo schema (${fileName}).`);

        }

        if (typeof parsedSchema.hashKey === 'undefined') {

            throw new Error(`Property 'hashKey' is required and missing from dynamo schema (${schema.tableName}).`);

        }

        if (typeof parsedSchema.schema[parsedSchema.hashKey] === 'undefined') {

            throw new Error(`Property '${parsedSchema.hashKey}' is not defined in the 'schema' property but used as the hashKey in dynamo schema (${schema.tableName}).`);

        }

        if (typeof parsedSchema.rangeKey !== 'undefined' && typeof parsedSchema.schema[parsedSchema.rangeKey] === 'undefined') {

            throw new Error(`Property '${parsedSchema.rangeKey}' is not defined in the 'schema' property but used as the rangeKey in dynamo schema (${schema.tableName}).`);

        }

        return parsedSchema;

    }

}

module.exports = DynamoTables;
