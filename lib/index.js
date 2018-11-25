'use strict';

const debug = require('debug')('layerize');
const knex = require('knex');
const elasticSearch = require('elasticsearch');
const { Base, Crud, Layers, SchemaBuilder, Schemas, Tables, JsonValidator } = require('./services');
const { errors, redis, objects } = require('./utils');
const { setLock } = redis;
class Layerize {

    constructor ({ schemas = '', keywords = '', formats = '', cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false, layerizeSchemaName = 'layerize' } = {}) {

        this.realTimeTransactions = realTimeTransactions;
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.error = errors;
        this.debug = debug;
        this.schemaPattern = schemas;
        this.keywordPattern = keywords;
        this.formatPattern = formats;
        this.systemSchemas = {
            raw: [],
            layerize: {}
        };
        this.schemas = {
            raw: [],
            layerize: {}
        };
        this.dbSchemas = {}; //schemas that exist in the database but may not be controller by layerize
        this.controlledSchemas = {}; //schemas that exist in the database that are controlled by layerize
        this.cache = redis.cache;
        this.es = null;
        this.db = null;
        this.layerizeSchemaName = layerizeSchemaName;
        this.systemValidator = new JsonValidator().validator;
        this.validator = new JsonValidator().validator;

    }

    static Base () {

        return Base;

    }

    static Crud () {

        return Crud;

    }

    async initiate ({ db = {}, cache = {}, es = {} } = {}) {

        this.debug('initiate()');
        try {

            /**
             * Database connection object
             * {
             *  client: 'pg',
             *  connection: {
             *      host: 'localhost',
             *      user: 'postgres',
             *      password: '',
             *      database: 'postgres'
             *  },
             *  pool: {
             *      min: 2,
             *      max: 10
             *  },
             *  acquireConnectionTimeout: 60000
             * }
             */

            if (Object.keys(db).length === 0) {

                throw new Error('A database connection definition \'db\' is requried when calling initiate({ db, cache, es, setup })');

            }

            this.db = knex(db);

            /**
             * Redis connection object
             * {
             *  host: 'localhost',
             *  port: 6379
             * }
             */

            if (Object.keys(cache).length === 0) {

                throw new Error('A cache connection definition \'cache\' is requried when calling initiate({ db, cache, es, setup })');

            }

            redis.init(cache.port, cache.host);

            /**
             * ElasticSearch connection object
             * {
             *  protocol: 'http'
             *  host: 'localhost',
             *  port: 9200
             * }
             */

            if (Object.keys(cache).length === 0) {

                throw new Error('A elasticsearch connection definition \'es\' is requried when calling initiate({ db, cache, es, setup })');

            }

            this.es = new elasticSearch.Client({ host: `${es.protocol}://${es.host}:${es.port}/` });
            this.es.ping({
                requestTimeout: 3000
            }, (err) => {

                if (err) {

                    throw new Error('can not connect to elastic search');

                }

            });

            /**
             * load layerize core table schemas
             * */
            let systemSchemas = new SchemaBuilder({ validator: this.systemValidator });
            this.systemSchemas = await systemSchemas.load('./lib/data/schemas/**/*.json');

            /**
             * load application table schemas
             * */
            let schemas = new SchemaBuilder({ validator: this.validator });
            this.schemas = await schemas.load(this.schemaPattern);

            /**
             * load system data table
             * */
            this.data = {
                schemas: new Schemas({ layerize: this, schemaName: this.layerizeSchemaName }),
                tables: new Tables({ layerize: this, schemaName: this.layerizeSchemaName })
            };

            /**
             * initiate layerizeCore, if it does not exist
             * */
            await this._initiateCore();

            // TODO: check to see if data layer updates need

            return this;

        } catch (error) {

            throw this.error.handle({ error, caller: 'connect' });

        }

    }

    layers ({ schemaName, schemas = this.schemas, realTimeTransactions = this.realTimeTransactions, layerizeCore = false, validator = this.validator } = {}) {

        this.debug('layers()');
        try {

            if (schemaName === this.layerizeSchemaName) {

                layerizeCore = true;

            }

            if (layerizeCore) {

                schemaName = this.layerizeSchemaName;
                schemas = this.systemSchemas;
                validator = this.systemValidator;

            }

            return new Layers({
                name: schemaName,
                schemas,
                database: this.db,
                cache: this.cache,
                search: this.es,
                validator,
                realTimeTransactions,
                cacheExpireSeconds: this.cacheExpireSeconds
            });

        } catch (error) {

            throw this.error.handle({ error, caller: 'layers' });

        }

    }

    async install ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('install');

        let unlock = () => {};

        try {

            let schemas = this.schemas;

            if (layerizeCore) {

                schemaName = this.layerizeSchemaName;
                schemas = this.systemSchemas;

            } else {

                if (schemaName === '') {

                    throw new Error('passing a schemaName is required when using install({ schemaName })');

                }

            }

            /**
             * lock so same request does not come in twice
             */
            unlock = await setLock(`LAYERIZE:LOCK:INSTALL:${schemaName.toUpperCase()}`, 60000);

            await this._buildTables({ schemaName, schemas, layerizeCore });

            this.dbSchemas = await this.loadSchemaListFromDB();

            /**
             * be sure to unlock so it will release the lock
             */
            unlock();

        } catch (error) {

            /**
             * be sure to unlock if there is an error so it will release the lock
             */
            unlock();
            throw this.error.handle({ error, caller: 'install' });

        }

    }

    async uninstall ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('uninstall');

        let unlock = () => {};

        try {

            if (layerizeCore) {

                schemaName = this.layerizeSchemaName;

            } else {

                if (schemaName === '') {

                    throw new Error('passing a schemaName is required when using uninstall({ schemaName })');

                }

            }

            /**
             * lock so same request does not come in twice
             */
            unlock = await setLock(`LAYERIZE:LOCK:INSTALL:${schemaName.toUpperCase()}`, 60000);

            await this.db.schema.raw(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);

            await this.es.indices.delete({
                index: `${schemaName}~*`
            });

            this.dbSchemas = await this.loadSchemaListFromDB();

            /**
             * be sure to unlock so it will release the lock
             */
            unlock();

            return true;

        } catch (error) {

            /**
             * be sure to unlock if there is an error so it will release the lock
             */
            unlock();
            throw this.error.handle({ error, caller: 'uninstall' });

        }

    }

    async loadControlledSchemas () {

        this.debug('loadControlledSchemas');
        try {

            let schemas = await this.data.schemas._search({ fields: 'id,name,hash,version', limit: 0 });
            this.controlledSchemas = objects.arrayToObject(schemas.items, 'name');

            return this.controlledSchemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadControlledSchemas' });

        }

    }

    async loadEnvironment () {

        this.debug('loadEnvironment()');
        try {

            // let exist = await this.db.schema.hasTable(table.hasName);

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadEnvironment' });

        }

    }

    async loadSchemaListFromDB () {

        this.debug('loadSchemaListFromDB');
        try {

            let results = await this.db.schema.raw('select nspname AS "name" from pg_catalog.pg_namespace n WHERE n.nspname !~ \'^pg_\' AND n.nspname <> \'information_schema\';');

            this.dbSchemas = objects.arrayToObject(results.rows, 'name');

            return this.dbSchemas;

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadSchemaListFromDB' });

        }

    }

    async _buildTables ({ schemaName = 'public', schemas = this.schemas, layerizeCore = false } = {}) {

        this.debug(`_buildTables({ schemaName: ${schemaName}})`);

        let unlock = () => {};
        let transaction = { rollback: async () => true };

        try {

            /**
             * lock so same request does not come in twice
             */
            unlock = await setLock(`LAYERIZE:LOCK:BUILDTABLES:${schemaName.toUpperCase()}`, 300000); // 5 min lock expire

            /**
             * do not assume loadSchemaListFromDB() has already run, so we are going to check
             */
            if (Object.keys(this.dbSchemas).length === 0) {

                this.dbSchemas = await this.loadSchemaListFromDB();

            }

            if (layerizeCore) {

                schemaName = this.layerizeSchemaName;
                schemas = this.systemSchemas;

            }

            let runSchema = true;
            let updateTables = false;
            if (typeof this.dbSchemas[schemaName] !== 'undefined') {

                /**
                 * do not assume loadSchemaListFromDB() has already run, so we are going to check
                 */
                if (Object.keys(this.controlledSchemas).length === 0) {

                    this.controlledSchemas = await this.loadControlledSchemas();

                }

                if (typeof this.controlledSchemas[schemaName] === 'undefined') {

                    throw new Error(`The schema '${schemaName}' already exists in the database but is not controlled by layerize. You must first remove the schema before layerize is able to recreate it.`);

                } else {

                    runSchema = this.controlledSchemas[schemaName].hash !== schemas.hash;
                    updateTables = true;

                }

            }

            if (runSchema) {

                /**
                 * creating a seperate system transaction since it is a different schema because layers within layerize our tied to a specific schema
                 */
                let systemLayers = this.layers({ layerizeCore: true });
                let systemTransaction = systemLayers.transaction();

                /**
                 * creating a transaction to handle the new schema
                 */
                let layers = this.layers({ schemaName, layerizeCore });
                transaction = layers.transaction();
                await transaction.rawDatabase(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);

                let objExistingTables = {};
                if (updateTables) {

                // get current version of tables, limit set to zero for returning all results
                    let existingTables = await this.data.tables._search({ filter: `schema_name:${schemaName}`, fields: 'id,name,hash,version', limit: 0 });
                    objExistingTables = objects.arrayToObject(existingTables.items, 'name');

                }

                let tables = Object.keys(schemas.layerize);
                for (let i = 0; i < tables.length; i++) {

                    let table = schemas.layerize[tables[i]];

                    if (typeof objExistingTables[table.name] === 'undefined') {

                        let strSQL = await this.__createTableSQL({ schemaName, table });

                        if (table.esEnabled) {

                            await this.es.indices.create({
                                index: `${schemaName}~${table.name}~${table.version}`,
                                body: {
                                    mappings: {
                                        index: table.esIndex
                                    },
                                    settings: {
                                        analysis: {
                                            normalizer: {
                                                product_name_normalizer: {
                                                    type: 'custom',
                                                    char_filter: [],
                                                    filter: [
                                                        'lowercase',
                                                        'asciifolding'
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            });

                        }

                        await transaction.rawDatabase(strSQL);

                        await this.data.tables._insert({
                            schema_name: schemaName,
                            name: table.name,
                            schema: table,
                            version: schemas.version,
                            hash: table.hash,
                            es_enabled: table.esEnabled
                        }, { transaction: systemTransaction });

                    } else if (objExistingTables[table.name].hash !== table.hash) {

                        let tableRecord = await this.data.tables._get(objExistingTables[table.name].id, { transaction: systemTransaction });

                        let strSQL = await this.__alterTableSQL({ schemaName, currentTable: tableRecord.schema, table });

                        await transaction.rawDatabase(strSQL);

                        await this.data.tables._update(tableRecord.id, {
                            schema: table,
                            version: schemas.version,
                            hash: table.hash,
                            es_enabled: table.esEnabled
                        }, { transaction: systemTransaction });

                    }
                    this.debug('complete');

                }

                if (updateTables) {

                    let schemaRecord = await this.data.schemas._get(this.controlledSchemas[schemaName].id, { transaction: systemTransaction });

                    await this.data.schemas._update(schemaRecord.id, {
                        version: schemas.version,
                        hash: schemas.hash
                    }, { transaction: systemTransaction });

                } else {

                    await this.data.schemas._insert({
                        name: schemaName,
                        version: schemas.version,
                        hash: schemas.hash
                    }, { transaction: systemTransaction });

                }

                /**
                 * combine the two transactions before committing
                 */
                transaction.combine(systemTransaction);

                await transaction.commit();
                this.dbSchemas = await this.loadSchemaListFromDB();

            }

            /**
             * be sure to unlock so it will release the lock
             */
            unlock();

            return true;

        } catch (error) {

            /**
             * be sure to unlock if there is an error so it will release the lock
             */
            unlock();

            /**
             * be sure to rollback if there is an error
             */
            await transaction.rollback();

            throw this.error.handle({ error, caller: '_buildTables' });

        }

    }

    async _initiateCore () {

        this.debug('_initiateCore');
        try {

            /**
             * load current list of database schemas
             * */
            this.dbSchemas = await this.loadSchemaListFromDB();

            await this.install({ layerizeCore: true });

        } catch (error) {

            throw this.error.handle({ error, caller: '_initiateCore' });

        }

    }

    async __alterTableSQL ({ schemaName = '', currentTable = {}, table = {} } = {}) {

        this.debug('__alterTableSQL');
        try {

            let currentColumns = currentTable.columns;

            let columns = Object.keys(table.columns);

            return this.db.schema.withSchema(schemaName).alterTable(table.name, (tbl) => {

                for (let i = 0; i < columns.length; i++) {

                    let column = table.columns[columns[i]];

                    if (typeof currentColumns[column.name] === 'undefined' || currentColumns[column.name].hash !== column.hash) {

                        this.__defineTableColumn({ column, tbl, alter: (typeof currentColumns[column.name] !== 'undefined') });

                    }

                }

                /**
                 * check for dropped columns
                 */
                let aryCurrentColumns = Object.keys(currentColumns);
                for (let x = 0; x < aryCurrentColumns.length; x++) {

                    if (typeof table.columns[aryCurrentColumns[x]] === 'undefined') {

                        this.__defineTableColumn({ column: currentColumns[aryCurrentColumns[x]], tbl, drop: true });

                    }

                }

            }).toString();

        } catch (error) {

            throw this.error.handle({ error, caller: '__alterTableSQL' });

        }

    }

    async __createTableSQL ({ schemaName = '', table = {} } = {}) {

        this.debug('__createTableSQL');
        try {

            let columns = Object.keys(table.columns);

            return this.db.schema.withSchema(schemaName).createTable(table.name, (tbl) => {

                for (let i = 0; i < columns.length; i++) {

                    let column = table.columns[columns[i]];

                    this.__defineTableColumn({ column, tbl });

                }

            }).toString();

        } catch (error) {

            throw this.error.handle({ error, caller: '__createTableSQL' });

        }

    }

    __defineTableColumn ({ column = {}, tbl, alter = false, drop = false } = {}) {

        this.debug('__defineTableColumn');
        try {

            let ctbl;

            if (drop) {

                return tbl.dropColumn(column.name);

            }

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

                    if (typeof column.default === 'object' && column.default !== null) {

                        ctbl.defaultTo(JSON.stringify(column.default));

                    } else {

                        ctbl.defaultTo(column.default);

                    }

                }

            }

            if (alter) {

                ctbl.alter();

            }

            return ctbl;

        } catch (error) {

            throw this.error.handle({ error, caller: '__defineTableColumn' });

        }

    }

}

module.exports = Layerize;
