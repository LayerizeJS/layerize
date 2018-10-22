'use strict';

const debug = require('debug')('layerize');
const glob = require('glob');
const fs = require('fs');
const knex = require('knex');
const extend = require('extend');
const elasticSearch = require('elasticsearch');
const { Layers } = require('./services');
const { layerizeSchema, jsonValidator, objects, errors, redis } = require('./utils');
const { parseRefs } = layerizeSchema;

class Layerize {

    constructor ({ schemas = '', keywords = '', formats = '', cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false } = {}) {

        this.realTimeTransactions = realTimeTransactions;
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.error = errors;
        this.debug = debug;
        this.schemaPattern = schemas;
        this.keywordPattern = keywords;
        this.formatPattern = formats;
        this.schemas = {
            raw: [],
            layerize: {}
        };
        this.cache = redis;
        this.es = null;
        this.db = null;

    }

    async initiate () {

        this.debug('initiate()');
        try {

            this.db = knex({
                client: 'pg',
                connection: {
                    host: '192.168.99.100',
                    user: 'postgres',
                    password: '',
                    database: 'postgres'
                },
                pool: {
                    min: 2,
                    max: 10,
                    afterCreate: (conn, done) => {

                        this.debug('new pool connection created');
                        done(null, conn);

                    }
                },
                acquireConnectionTimeout: 60000
            });

            this.cache.init(6379, '192.168.99.100');

            this.es = new elasticSearch.Client({ host: 'http://192.168.99.100:9200/' });
            this.es.ping({
                requestTimeout: 30000
            }, (err) => {

                if (err) {

                    throw new Error('can not connect to elastic search');

                }

            });

            // connect to all data layers
            //   - database
            //   - cache
            //   - search

            // load schemas

            // check to see if data layer updates need

            await this.loadSchemas();

            // let layers = this.layers({ schema: 'public2' });
            // let transaction = layers.transaction();
            // let insertResults = await transaction.insert('users', [
            //     { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe10', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: 2 } ], custom_fields: { pickle: true } },
            //     { user_role_id: 'a8988288-988a-412a-9127-e51a284e2b46', first_name: 'John', last_name: ' Doe ', username: 'johndoe11', password: 'Mypassword1', email: 'pickle@dsfsd.com', system_keys: [ { key: '1', value: 2 } ], custom_fields: { pickle: true } }
            // ]);
            // await transaction.commit();
            // this.debug(insertResults);

            // let results = await layers.search('users', { filter: ['archived::false', 'first_name::John'] });
            // this.debug(results);

            let layers = this.layers({ schema: 'public2' });
            // let users = await layers.search('users');

            // let layers = this.layers({ schema: 'public2' });
            // let users = await layers.search('users', { fields: 'id', sort: 'username', filter: ['archived:false', 'first_name:John&&system_keys:![{"key":"2"}]'] });
            // this.debug(users);

            let user = await layers.get('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
            this.debug(user);

            // await layers.clearTableCache('users');

            // user.username = 'pickler';
            // this.debug(await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true }));

            // this.debug(await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { first_name: 'John3' }, { returnRecord: true }));

            // await this.buildTables();

        } catch (error) {

            throw this.error.handle({ error, caller: 'connect' });

        }

    }

    layers ({ schema, realTimeTransactions = this.realTimeTransactions } = {}) {

        this.debug('layers()');
        try {

            return new Layers({
                name: schema,
                schemas: this.schemas,
                database: this.db,
                cache: this.cache.cache,
                search: this.es,
                validator: jsonValidator,
                realTimeTransactions,
                cacheExpireSeconds: this.cacheExpireSeconds
            });

        } catch (error) {

            throw this.error.handle({ error, caller: 'layers' });

        }

    }

    async loadSchemas () {

        this.debug('loadSchemas()');
        try {

            let schemaFiles = glob.sync(this.schemaPattern);
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

                        this.schemas.raw.push(schema);

                    }

                }

            }

            // this.debug(JSON.stringify(this.schemas.raw));
            // this.debug(jsonValidator.getSchema('address2'));
            return await this.layerizeSchema();

        } catch (error) {

            throw this.error.handle({ error, caller: 'connect' });

        }

    }

    async layerizeSchema () {

        this.debug('layerizeSchema()');
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

            for (let i = 0; i < this.schemas.raw.length; i++) {

                let schema = this.schemas.raw[i];
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
                this.schemas.layerize[table.name] = table;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'layerizeSchema' });

        }

    }

    async buildTables () {

        this.debug('buildTables()');
        try {

            for (let i = 0; i < this.schemas.layerize.length; i++) {

                let table = this.schemas.layerize[i];

                // let exist = await this.db.schema.hasTable(table.hasName)

                // if (exist === false) {

                //     await this.db.schema.createTable(table.hasName, (table) => {
                //         table.increments();
                //     });

                // }

                /**
                     *  - mark schema as being updated
                     *
                     * */

                // CREATE SCHEMA IF NOT EXISTS public2;

                await this.db.schema.raw('CREATE SCHEMA IF NOT EXISTS public2').withSchema('public2').createTable(table.name, (tbl) => {

                    for (let i = 0; i < table.columns.length; i++) {

                        let column = table.columns[i];

                        let ctbl;
                        if (column.serial) {

                            ctbl = tbl.specificType(column.name, 'serial');

                        } else {

                            ctbl = tbl[column.type](column.name, column.length);

                        }

                        if (column.nullable) {

                            ctbl.nullable();

                        } else {

                            ctbl.notNullable();

                        }

                        if (column.primary) {

                            ctbl.primary();

                        }

                        if (column.unique) {

                            ctbl.unique();

                        }

                        if (typeof column.default !== 'undefined') {

                            if (column.default === 'now()') {

                                ctbl.defaultTo(this.db.fn.now());

                            } else {

                                ctbl.defaultTo(JSON.stringify(column.default));

                            }

                        }

                    }

                });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'buildTables' });

        }

    }

    // search(filter)

    // get(id)

    // update(id || filter)

    // insert()

}

module.exports = Layerize;
