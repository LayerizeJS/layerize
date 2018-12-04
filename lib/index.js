
'use strict';

const debug = require('debug')('layerize');
const knex = require('knex');
const elasticSearch = require('elasticsearch');
const { Base, Crud, Layers, SchemaBuilder, Schemas, Tables, JsonValidator } = require('./services');
const { errors, redis, objects } = require('./utils');
const { setLock } = redis;
const { warn } = console;
class Layerize {

    constructor ({ schemas = '', keywords = '', formats = '', cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false, layerizeSchemaName = 'layerize', coerceTypes = false } = {}) {

        this.realTimeTransactions = realTimeTransactions;
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.error = errors;
        this.debug = debug;
        this.schemaPattern = schemas;
        this.keywordPattern = keywords;
        this.formatPattern = formats;
        this.coerceTypes = coerceTypes;
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
        this.validator = new JsonValidator({ coerceTypes: this.coerceTypes }).validator;

    }

    static Base () {

        return Base;

    }

    static Crud () {

        return Crud;

    }

    async initiate ({ db = {}, cache = {}, es = {}, updateAll = false, install = false, schemaName = 'public' } = {}) {

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

            /**
             * install/update the schema/schemas defined by schemaName
             * */
            if (install) {

                await this.install({ schemaName });

            }

            /**
             * update all currently installed schemas
             * */
            if (updateAll) {

                await this.updateAll();

            }

            /**
             * TODO: check to see if data layer updates need
             * */
            await this.validateSchemaVersions();

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

                if ((Array.isArray(schemaName) && schemaName.length === 0) || schemaName === '') {

                    throw new Error('passing a schemaName is required when using install({ schemaName })');

                }

            }

            let arySchemaNames = [];
            if (!Array.isArray(schemaName)) {

                arySchemaNames.push(schemaName);

            } else {

                arySchemaNames = schemaName;

            }

            for (let i = 0; i < arySchemaNames.length; i++) {

                let schemaName = arySchemaNames[i];
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

            }

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

            /**
             * drop database schema and tables
             */
            await this.db.schema.raw(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);

            /**
             * delete all related ES schemas
             */
            await this.es.indices.delete({ index: `${schemaName}~*` });

            /**
             * TODO: delete all related schemas caches
             */

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

    async updateAll () {

        this.debug('updateAll');
        try {

            let arySchemas = Object.keys(this.controlledSchemas);
            for (let i = 0; i < arySchemas.length; i++) {

                await this.install({ schemaName: this.controlledSchemas[arySchemas[i]] });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'updateAll' });

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

    async validateSchemaVersions () {

        this.debug('validateSchemaVersions');
        try {

            let arySchemas = Object.keys(this.controlledSchemas);
            for (let i = 0; i < arySchemas.length; i++) {

                let schema = this.controlledSchemas[arySchemas[i]];
                if (schema.hash !== this.schemas.hash) {

                    warn(`WARNING: Schema '${schema.name}' is needs to be updated`, schema.hash, this.schemas.hash);

                }

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'validateSchemaVersions' });

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
        let systemTransaction = { rollback: async () => true };

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

                    throw new Error(`The schema '${schemaName}' already exists in the database but is not controlled by layerize. You must first remove the schema before layerize is able to recreate and control it.`);

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

                let schemaRecord = {};
                let objExistingTables = {};
                if (updateTables) {

                    schemaRecord = await this.data.schemas._get(this.controlledSchemas[schemaName].id, { transaction: systemTransaction });

                    if (schemaRecord.version === schemas.version) {

                        throw new Error('Schema version number needs to be incremented for changes to apply.');

                    }

                    // get current version of tables, limit set to zero for returning all results
                    let existingTables = await this.data.tables._search({ filter: `schema_name:${schemaName}`, fields: 'id,name,hash,version', limit: 0 });
                    objExistingTables = objects.arrayToObject(existingTables.items, 'name');

                }

                let tables = Object.keys(schemas.layerize);
                let foreignKeys = {};
                for (let i = 0; i < tables.length; i++) {

                    let table = schemas.layerize[tables[i]];

                    let esIndexName = `${schemaName}~${table.name}~${table.version}`;
                    let esIndexAlias = `${schemaName}~${table.name}`;

                    if (typeof objExistingTables[table.name] === 'undefined') {

                        let strSQL;
                        ({ sql: strSQL, foreignKeys } = await this.__createTableSQL({ schemaName, table, foreignKeys }));

                        if (table.esEnabled) {

                            await this.es.indices.create({
                                index: esIndexName,
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

                            /**
                             * Map aliases
                             */
                            await this.es.indices.updateAliases({
                                body: {
                                    actions: [
                                        {
                                            add: {
                                                index: esIndexName,
                                                alias: esIndexAlias
                                            }
                                        }
                                    ]
                                }
                            });

                        }

                        await transaction.rawDatabase(strSQL);

                        await this.data.tables._insert({
                            schema_name: schemaName,
                            name: table.name,
                            schema: table,
                            version: table.version,
                            hash: table.hash,
                            es_enabled: table.esEnabled,
                            cache_enabled: true,
                            es_index_name: (table.esEnabled) ? esIndexName : null
                        }, { transaction: systemTransaction });

                    } else if (objExistingTables[table.name].hash !== table.hash) {

                        if (objExistingTables[table.name].version === schemas.version) {

                            throw new Error('Table version number needs to be incremented for changes to apply.');

                        }

                        let tableRecord = await this.data.tables._get(objExistingTables[table.name].id, { transaction: systemTransaction });

                        let strSQL;
                        ({ sql: strSQL, foreignKeys } = await this.__alterTableSQL({ schemaName, currentTable: tableRecord.schema, table, foreignKeys }));

                        await layers.clearTableCache(table.name);

                        if (table.esEnabled && tableRecord.es_enabled === false) {

                            /**
                             * create ES index
                             */
                            await this.es.indices.create({
                                index: esIndexName,
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

                            /**
                             * Map aliases
                             */
                            await this.es.indices.updateAliases({
                                body: {
                                    actions: [
                                        {
                                            add: {
                                                index: esIndexName,
                                                alias: esIndexAlias
                                            }
                                        }
                                    ]
                                }
                            });

                            /**
                             * TODO: load data into ES index
                             */

                        } else if (table.esEnabled === false && tableRecord.es_enabled === true) {

                            /**
                             * remove map aliases
                             */
                            await this.es.indices.updateAliases({
                                body: {
                                    actions: [
                                        {
                                            remove: {
                                                index: tableRecord.es_index_name,
                                                alias: esIndexAlias
                                            }
                                        }
                                    ]
                                }
                            });

                            /**
                             * removed ES index
                             */
                            await this.es.indices.delete({ index: `${schemaName}~${table.name}~*` });

                        } else {

                            /**
                             * update ES index by replacing and reindexing
                             */
                            await this.es.indices.create({
                                index: esIndexName,
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

                            /**
                             * Copy documents to newly created index
                             */
                            await this.es.reindex({
                                body: {
                                    source: {
                                        index: tableRecord.es_index_name
                                    },
                                    dest: {
                                        index: esIndexName
                                    }
                                }
                            });

                            /**
                             * Map aliases
                             */
                            await this.es.indices.updateAliases({
                                body: {
                                    actions: [
                                        {
                                            remove: {
                                                index: tableRecord.es_index_name,
                                                alias: esIndexAlias
                                            }
                                        },
                                        {
                                            add: {
                                                index: esIndexName,
                                                alias: esIndexAlias
                                            }
                                        }
                                    ]
                                }
                            });

                            /**
                             * removed ES index
                             */
                            await this.es.indices.delete({ index: tableRecord.es_index_name });

                            /**
                             * TODO: reload data if new property added to ES index that was already existing in table
                             */

                        }

                        await transaction.rawDatabase(strSQL);

                        await this.data.tables._update(tableRecord.id, {
                            schema: table,
                            version: table.version,
                            hash: table.hash,
                            es_enabled: table.esEnabled,
                            cache_enabled: true,
                            es_index_name: (table.esEnabled) ? esIndexName : null
                        }, { transaction: systemTransaction });

                    }

                }

                let foreignKeyTables = Object.keys(foreignKeys);
                for (let i = 0; i < foreignKeyTables.length; i++) {

                    let tableName = foreignKeyTables[i];
                    let strSQL = await this.__applyForeignKeys({ schemaName, tableName, foreignKeys: foreignKeys[tableName] });
                    await transaction.rawDatabase(strSQL);

                }

                if (updateTables) {

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
            if (transaction.hasBeenCombined === false) {

                systemTransaction.rollback();

            }

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

    async __alterTableSQL ({ schemaName = '', currentTable = {}, table = {}, foreignKeys = {} } = {}) {

        this.debug('__alterTableSQL');
        try {

            let currentColumns = currentTable.columns;

            let columns = Object.keys(table.columns);

            let sql = this.db.schema.withSchema(schemaName).alterTable(table.name, (tbl) => {

                for (let i = 0; i < columns.length; i++) {

                    let column = table.columns[columns[i]];

                    if (typeof currentColumns[column.name] === 'undefined' || currentColumns[column.name].hash !== column.hash) {

                        ({ foreignKeys } = this.__defineTableColumn({ column, tbl, table, foreignKeys, alter: (typeof currentColumns[column.name] !== 'undefined') }));

                    }

                }

                /**
                 * check for dropped columns
                 */
                let aryCurrentColumns = Object.keys(currentColumns);
                for (let x = 0; x < aryCurrentColumns.length; x++) {

                    if (typeof table.columns[aryCurrentColumns[x]] === 'undefined') {

                        ({ foreignKeys } = this.__defineTableColumn({ column: currentColumns[aryCurrentColumns[x]], tbl, table, foreignKeys, drop: true }));

                    }

                }

            }).toString();

            return { sql, foreignKeys };

        } catch (error) {

            throw this.error.handle({ error, caller: '__alterTableSQL' });

        }

    }

    async __applyForeignKeys ({ schemaName = '', tableName = '', foreignKeys = {} } = {}) {

        this.debug('__applyForeignKeys');
        try {

            return this.db.schema.withSchema(schemaName).alterTable(tableName, (tbl) => {

                let properties = Object.keys(foreignKeys);

                for (let i = 0; i < properties.length; i++) {

                    let property = properties[i];
                    let reference = foreignKeys[property];
                    tbl.foreign(property).references(reference.column).inTable(`${schemaName}.${reference.table}`);

                }

            }).toString();

        } catch (error) {

            throw this.error.handle({ error, caller: '__applyForeignKeys' });

        }

    }

    async __createTableSQL ({ schemaName = '', table = {}, foreignKeys = {} } = {}) {

        this.debug('__createTableSQL');
        try {

            let columns = Object.keys(table.columns);

            let sql = this.db.schema.withSchema(schemaName).createTable(table.name, (tbl) => {

                for (let i = 0; i < columns.length; i++) {

                    let column = table.columns[columns[i]];

                    ({ foreignKeys } = this.__defineTableColumn({ column, tbl, table, foreignKeys }));

                }

            }).toString();

            return { sql, foreignKeys };

        } catch (error) {

            throw this.error.handle({ error, caller: '__createTableSQL' });

        }

    }

    __defineTableColumn ({ column = {}, tbl, table = {}, alter = false, drop = false, foreignKeys = {} } = {}) {

        this.debug('__defineTableColumn');
        try {

            let ctbl;

            if (drop) {

                return { ctbl: tbl.dropColumn(column.name), foreignKeys };

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

            if (typeof column.foreign !== 'undefined') {

                // ctbl.references(column.foreign.reference.column).inTable(column.foreign.reference.table);

                if (typeof foreignKeys[table.name] === 'undefined') {

                    foreignKeys[table.name] = {};

                }

                foreignKeys[table.name][column.foreign.reference.property] = {
                    table: column.foreign.reference.table,
                    column: column.foreign.reference.column
                };

            }

            if (alter) {

                ctbl.alter();

            }

            return { ctbl, foreignKeys };

        } catch (error) {

            throw this.error.handle({ error, caller: '__defineTableColumn' });

        }

    }

}

module.exports = Layerize;
