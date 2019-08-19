
'use strict';

const debug = require('debug')('layerize');
const knex = require('knex');
const elasticSearch = require('elasticsearch');
const dynamodb = require('dynamodb');
const services = require('./services');
const path = require('path');
const { Base, Crud, Layers, Permissions, SchemaBuilder, Schemas, Tables, JsonValidator, DynamoTables } = services;
const utils = require('./utils');
const { errors, redis, objects, crypto } = utils;
const { md5 } = crypto;
const { setLock } = redis;
const { warn } = console;

//Overwrite pg package default date parsing to return string instead of a date object by default.
const pgTypes = require('pg').types;
const DATE_OID = 1082;
const TIMESTAMP_OID = 1114;
const TIMESTAMPTZ_OID = 1184;
pgTypes.setTypeParser(DATE_OID, (value) => new Date(value).toISOString());
pgTypes.setTypeParser(TIMESTAMP_OID, (value) => new Date(value).toISOString());
pgTypes.setTypeParser(TIMESTAMPTZ_OID, (value) => new Date(value).toISOString());

/**
 * The Layerize class is the main class and is your doorway into all other offered classes.
 */
class Layerize {

    /**
     * Create a instance of Layerize.
     * @param {object} config - available options
     */
    constructor ({ schemas = '', keywords = '', formats = '', cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false, layerizeSchemaName = 'layerize', layerizeEncryptionKey = '', coerceTypes = false, removeAdditionalProperties = true, auditLogs = true } = {}) {

        this.realTimeTransactions = Boolean(realTimeTransactions);
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.error = errors;
        this.debug = debug;
        this.schemaPattern = schemas;
        this.keywordPattern = keywords;
        this.formatPattern = formats;
        this.coerceTypes = Boolean(coerceTypes);
        this.removeAdditionalProperties = Boolean(removeAdditionalProperties);
        this.auditLogs = Boolean(auditLogs);
        this.systemSchemas = {
            raw: [],
            layerize: {}
        };
        this.schemas = {
            raw: [],
            layerize: {}
        };
        this.permissions = {
            hash: '',
            definitionGlob: '',
            definitions: {},
            validator: new JsonValidator({ coerceTypes: this.coerceTypes, removeAdditional: this.removeAdditionalProperties, utils, layerizeEncryptionKey }).validator,
            schemas: {},
            connector: null,
            getRole: null
        };
        this.connectionDetails = {
            db: {},
            cache: {},
            es: {}
        };
        this.watchers = {};
        this.dbSchemas = {}; //schemas that exist in the database but may not be controller by layerize
        this.controlledSchemas = {}; //schemas that exist in the database that are controlled by layerize
        this.cache = redis.cache;
        this.redis = null;
        this.es = null;
        this.db = null;
        this.dynamo = null;
        this.dynamoTables = null;
        this.layerizeSchemaName = layerizeSchemaName;
        this.systemValidator = new JsonValidator({ utils, layerizeEncryptionKey }).validator;
        this.validator = new JsonValidator({ coerceTypes: this.coerceTypes, utils, layerizeEncryptionKey }).validator;

    }

    /**
     * Initializes the instance of Layerize.
     * @param {object} config - available options
     * @param {object} config.db - database object.
     * @param {object} config.cache - redis object.
     * @param {object} config.es - elasticsearch object.
     * @param {object} config.dynamo - dynamodb object.
     * @param {boolean} [config.updateAll=false] - auto updates all schemas.
     * @param {boolean} [config.install=false] - auto updates all schemas.
     * @param {string} [config.schemaName='public'] - the schemaName to install.
     * @param {boolean} [config.skipCoreInstall=false] - when true it does not install/update the core, good for clusters.
     * @param {object} [config.permissions={}] - permissions settings object.
     * @param {array} [config.watchers={}] - array of watcher objects, watchers are processed realtime.
     * @returns {Promise<Layerize>} the instance of itself
     * @throws {error} A database connection definition 'db' is requried when calling initiate({ db, cache, es, setup }).
     * @throws {error} A cache connection definition 'cache' is requried when calling initiate({ db, cache, es, setup }).
     * @throws {error} A elasticsearch connection definition 'es' is requried when calling initiate({ db, cache, es, setup }).
     * @throws {error} Unable to connect to elastic search, please verify it is running.
     */
    async initiate ({ db = {}, cache = {}, es = {}, dynamo = {}, updateAll = false, install = false, schemaName = 'public', skipCoreInstall = false, permissions = {}, watchers = [] } = {}) {

        this.debug('initiate()');
        try {

            if (Object.keys(permissions).length > 0) {

                this.permissions.definitionGlob = permissions.definitionGlob || '';
                this.permissions.connector = permissions.connector || null;
                this.permissions.getRole = permissions.getRole || null;

            }

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

                throw new Error('A database connection definition \'db\' is requried when calling initiate({ db, cache, es, setup }).');

            }

            this.connectionDetails.db = db;
            this.db = knex(db);

            /**
             * Redis connection object
             * {
             *  host: 'localhost',
             *  port: 6379
             * }
             */

            if (Object.keys(cache).length === 0) {

                throw new Error('A cache connection definition \'cache\' is requried when calling initiate({ db, cache, es, setup }).');

            }

            this.connectionDetails.cache = cache;

            // initialize and store thr raw redis connection
            this.redis = redis.init(cache.port, cache.host);

            /**
             * ElasticSearch connection object
             * {
             *  protocol: 'http'
             *  host: 'localhost',
             *  port: 9200
             * }
             */

            if (Object.keys(es).length === 0) {

                throw new Error('A elasticsearch connection definition \'es\' is requried when calling initiate({ db, cache, es, setup }).');

            }

            this.connectionDetails.es = es;
            this.es = new elasticSearch.Client({ host: `${es.protocol}://${es.host}:${es.port}/` });
            this.es.ping({
                requestTimeout: 10000
            }, (err) => {

                if (err) {

                    throw new Error('Unable to connect to elastic search, please verify it is running.');

                }

            });

            if (Object.keys(dynamo).length > 0 && typeof dynamo.endpoint !== 'undefined' && dynamo.endpoint !== '') {

                if (typeof dynamo.accessKeyId === 'undefined' || dynamo.accessKeyId === '' || typeof dynamo.secretAccessKey === 'undefined' || dynamo.secretAccessKey === '') {

                    throw new Error('If dynamo endpoint is defined then accessKeyId and secretAccessKey must also be defined.');

                }

                this.dynamo = dynamodb;
                this.dynamo.AWS.config.update(dynamo);
                this.dynamo.documentClient(new this.dynamo.AWS.DynamoDB.DocumentClient({
                    service: this.dynamo.dynamodb,
                    convertEmptyValues: true
                }));

                this.dynamoTables = new DynamoTables({ dynamo: this.dynamo });
                await this.dynamoTables.load(path.resolve(__dirname, './data/dynamo/**/*.json'));

            }

            /**
             * load layerize core table schemas
             * */
            let systemSchemas = new SchemaBuilder({ validator: this.systemValidator });

            this.systemSchemas = await systemSchemas.load(path.resolve(__dirname, './data/schemas/**/*.json'));

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
             * load permission definitions if provided
             * */
            if (this.permissions.definitionGlob !== '') {

                let permissions = new Permissions({ layerize: this });
                this.permissions.service = permissions;
                this.permissions.definitions = await this.permissions.service.loadDefinitions(this.permissions.definitionGlob);
                this.permissions.hash = objects.hash(this.permissions.definitions);
                // console.log(JSON.stringify(this.permissions.definitions));
                // console.log(JSON.stringify(await permissions.generateAdminPermissions()));

            }

            /**
             * initiate layerizeCore, if it does not exist
             * */
            await this._initiateCore({ skipCoreInstall });

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

            // set watchers
            if (Array.isArray(watchers) && watchers.length > 0) {

                this._setWatchers({ watchers });

            }

            return this;

        } catch (error) {

            throw this.error.handle({ error, caller: 'connect' });

        }

    }

    /**
     * Create and Initializes a new instance of Layers.
     * @param {object} config - available options
     * @param {string} config.schemaName - name of the schema being used within the layer
     * @param {object} [config.schemas=schemas] - pass in a different schema to use.
     * @param {boolean} [config.realTimeTransactions=realTimeTransactions] - overwrite the flag from the default.
     * @param {boolean} [config.layerizeCore=false] - when true it will use the underline sytem core schema the layerize uses for administration.
     * @param {JsonValidator} [config.validator=validator] - pass in a different validator to use.
     * @param {object} [config.permissions={}] - permissioon.
     * @param {string} [config.user_id=''] - user_id.
     * @returns {Layers} a new instance of Layers
     */
    layers ({ schemaName, schemas = this.schemas, realTimeTransactions = this.realTimeTransactions, layerizeCore = false, validator = this.validator, permissions = {}, user_id = '' } = {}) {

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
                layerize: this,
                name: schemaName,
                schemas,
                database: this.db,
                cache: this.cache,
                search: this.es,
                dynamoTables: this.dynamoTables,
                validator,
                realTimeTransactions,
                cacheExpireSeconds: this.cacheExpireSeconds,
                permissions,
                user_id
            });

        } catch (error) {

            throw this.error.handle({ error, caller: 'layers' });

        }

    }

    /**
     * Install or updates the provide schema name.
     * @param {object} config - available options
     * @param {string} config.schemaName - name of the schema being used within the layer
     * @param {boolean} [config.layerizeCore=false] - when true it will use the underline sytem core schema the layerize uses for administration.
     * @returns {Promise<success>} returns a success object
     */
    async install ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('install');

        let unlock;

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

            return { success: true };

        } catch (error) {

            /**
             * be sure to unlock if there is an error so it will release the lock
             */
            if (typeof unlock !== 'undefined') {

                unlock();

            }
            throw this.error.handle({ error, caller: 'install' });

        }

    }

    /**
     * Uninstall the provide schema name.
     * @param {object} config - available options
     * @param {string} config.schemaName - name of the schema being used within the layer
     * @param {boolean} [config.layerizeCore=false] - when true it will use the underline sytem core schema the layerize uses for administration.
     * @returns {Promise<success>} returns a success object
     */
    async uninstall ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('uninstall');

        let unlock;

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
             * delete all related schemas caches
             */
            let layers = this.layers({ schemaName });
            await layers.clearAllTablesCache();

            /**
             * delete all permission caches for schemaName
             */
            await this.cache.deleteByPattern(this.cache.key('LAYERIZE:PERMISSIONS', schemaName, '*'));

            /**
             * drop database schema and tables
             */
            await this.db.schema.raw(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);

            /**
             * delete all related ES schemas
             */
            await this.es.indices.delete({ index: `${schemaName}~*` });

            this.dbSchemas = await this.loadSchemaListFromDB();

            if (this.dynamo !== null) {

                /**
                 * delete all dynamoTables
                 */
                await this.dynamoTables.table('audit-logs', schemaName).deleteTable();

            }

            /**
             * be sure to unlock so it will release the lock
             */
            unlock();

            return { success: true };

        } catch (error) {

            /**
             * be sure to unlock if there is an error so it will release the lock
             */
            if (typeof unlock !== 'undefined') {

                unlock();

            }
            throw this.error.handle({ error, caller: 'uninstall' });

        }

    }

    /**
     * Updates all database schemas controlled by LayerizeJS.
     * @returns {Promise<success>} returns a success object
     */
    async updateAll () {

        this.debug('updateAll');
        try {

            let arySchemas = Object.keys(this.controlledSchemas);
            for (let i = 0; i < arySchemas.length; i++) {

                if (arySchemas[i] !== this.layerizeSchemaName) {

                    await this.install({ schemaName: arySchemas[i] });

                }

            }

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'updateAll' });

        }

    }

    /**
     * Loads all schemas controlled by LayerizeJS.
     * @returns {Promise<object>} returns an object
     */
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

    /**
     * Checks to make sure no controller database schema need updating.
     * @returns {Promise<success>} returns an object
     */
    async validateSchemaVersions () {

        this.debug('validateSchemaVersions');
        try {

            let arySchemas = Object.keys(this.controlledSchemas);
            for (let i = 0; i < arySchemas.length; i++) {

                let schema = this.controlledSchemas[arySchemas[i]];
                let compareSchema = this.schemas;

                if (arySchemas[i] === this.layerizeSchemaName) {

                    compareSchema = this.systemSchemas;

                }

                if (schema.hash !== compareSchema.hash) {

                    warn(`WARNING: Schema '${schema.name}' needs to be updated, as changes exist!`);

                }

            }

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'validateSchemaVersions' });

        }

    }

    /**
     * Gets raw list from database of all schemas being used.
     * @returns {Promise<object>} returns an object
     */
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

    /**
     * A protected method for building the tables in each data layer
     * @access protected
     * @param {object} config - available options
     * @param {string} config.schemaName - name of the schema being used within the layer
     * @param {object} [config.schemas=schemas] - pass in a different schema to use.
     * @param {boolean} [config.layerizeCore=false] - when true it will use the underline sytem core schema the layerize uses for administration.
     * @returns {Layers} a new instance of Layers
     * @throws {error} The schema '{schemaName}' already exists in the database but is not controlled by layerize. You must first remove the schema before layerize is able to recreate and control it.
     * @throws {error} Schema version number needs to be incremented for changes to apply.
     * @throws {error} Table version number needs to be incremented for changes to apply.
     */
    async _buildTables ({ schemaName = 'public', schemas = this.schemas, layerizeCore = false } = {}) {

        this.debug(`_buildTables({ schemaName: ${schemaName}})`);

        let unlock;
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
                systemTransaction = systemLayers.transaction();

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
                                                case_insensitive_normalizer: {
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
                            try {

                                await this.es.indices.create({
                                    index: esIndexName,
                                    body: {
                                        mappings: {
                                            index: table.esIndex
                                        },
                                        settings: {
                                            analysis: {
                                                normalizer: {
                                                    case_insensitive_normalizer: {
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

                            } catch (err) {

                                // index was already created so allow it to continue
                                if (err.body.error.type !== 'resource_already_exists_exception') {

                                    throw err;

                                }

                            }

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
                            try {

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

                            } catch (err) {
                                // do nothing as it may no longer exist for other reasons
                            }

                            /**
                             * removed ES index
                             */
                            try {

                                await this.es.indices.delete({ index: `${schemaName}~${table.name}~*` });

                            } catch (err) {
                                // do nothing as it may no longer exist for other reasons
                            }

                        } else if (table.esEnabled === true && tableRecord.es_enabled === true) {

                            /**
                             * update ES index by replacing and reindexing
                             */
                            debug('creating ES index: ', esIndexName);
                            try {

                                await this.es.indices.create({
                                    index: esIndexName,
                                    body: {
                                        mappings: {
                                            index: table.esIndex
                                        },
                                        settings: {
                                            analysis: {
                                                normalizer: {
                                                    case_insensitive_normalizer: {
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

                            } catch (err) {

                                // index was already created so allow it to continue
                                if (err.body.error.type !== 'resource_already_exists_exception') {

                                    throw err;

                                }

                            }

                            /**
                             * Copy documents to newly created index
                             */
                            try {

                                debug(`reindex ES index: ${tableRecord.es_index_name} to ${esIndexName}`);
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

                            } catch (err) {

                                // index was already created so allow it to continue
                                if (err.body.error.type !== 'index_not_found_exception') {

                                    throw err;

                                }

                            }

                            /**
                             * Map aliases
                             */
                            debug(`update ES alias: ${tableRecord.es_index_name} to ${esIndexName}`);
                            try {

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

                            } catch (err) {

                                // index was already created so allow it to continue
                                if (err.body.error.type !== 'index_not_found_exception') {

                                    throw err;

                                }

                            }

                            /**
                             * removed ES index
                             */
                            debug(`delete ES index: ${tableRecord.es_index_name}`);
                            try {

                                await this.es.indices.delete({ index: tableRecord.es_index_name });

                            } catch (err) {

                                // index was already created so allow it to continue
                                if (err.body.error.type !== 'index_not_found_exception') {

                                    throw err;

                                }

                            }

                            /**
                             * TODO: reload data if new property added to ES index that was already existing in table
                             */

                        }

                        await transaction.rawDatabase(strSQL);

                        await this.data.tables._patch(tableRecord.id, {
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

                    await this.data.schemas._patch(schemaRecord.id, {
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

                if (this.dynamo !== null) {

                    /**
                     * creete all dynamoTablestables
                     */
                    await this.dynamoTables.table('audit-logs', schemaName);

                    await this.dynamo.createTables();

                }

                await transaction.commit();

                this.controlledSchemas = await this.loadControlledSchemas();
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
            if (typeof unlock !== 'undefined') {

                unlock();

            }

            /**
             * be sure to rollback if there is an error
             */
            if (transaction.hasBeenCombined === false) {

                await systemTransaction.rollback();

            }

            await transaction.rollback();

            throw this.error.handle({ error, caller: '_buildTables' });

        }

    }

    /**
     * A protected method for initializing the LayerizeJS core.
     * @access protected
     * @param {object} config - available options
     * @param {boolean} [config.skipCoreInstall=false] - when true it does not install/update the core, good for clusters
     * @returns {Promise<success>} returns a success object
     */
    async _initiateCore ({ skipCoreInstall = false }) {

        this.debug('_initiateCore');
        try {

            /**
             * load current list of database schemas
             * */
            this.dbSchemas = await this.loadSchemaListFromDB();

            if (skipCoreInstall === false) {

                await this.install({ layerizeCore: true });

            }

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: '_initiateCore' });

        }

    }

    /**
     * A protected method for settting/adding watchers.
     * @access protected
     * @param {object} config - available options
     * @param {array} [config.watchers=[]] - watchers to add
     * @returns {object} returns a success object
     */
    _setWatchers ({ watchers = [] } = {}) {

        this.debug('_setWatchers');
        try {

            for (let i = 0; i < watchers.length; i++) {

                let watcher = watchers[i];

                if (typeof watcher.table === 'undefined' || watcher.table === '') {

                    throw new Error('\'table\' property must be defined on a watcher.');

                }

                if (typeof watcher.on === 'undefined' || watcher.on === '' || (Array.isArray(watcher.on) && watcher.on.length === 0)) {

                    throw new Error('\'on\' property must be defined on a watcher.');

                } else {

                    if (!Array.isArray(watcher.on)) {

                        watcher.on = [ watcher.on ];

                    }

                    for (let x = 0; x < watcher.on.length; x++) {

                        let on = watcher.on[x];
                        switch (on) {

                            case 'insert':
                            case 'update':
                            case 'delete':
                            case '*':
                                break;

                            default:

                                throw new Error(`watcher on='${on}' is not an allowed type, must be ['insert', 'update', 'delete', '*']`);

                        }

                    }

                }

                if (typeof watcher.action !== 'function') {

                    throw new Error('\'action\' property must be defined on a watcher and must be a function.');

                }

                if (typeof this.schemas.layerize[watcher.table] === 'undefined') {

                    throw new Error(`watcher table='${watcher.table}' is not found`);

                }

                if (typeof this.watchers[watcher.table] === 'undefined') {

                    this.watchers[watcher.table] = {};

                }

                let actionHash = md5(watcher.action.toString());

                for (let x = 0; x < watcher.on.length; x++) {

                    let on = watcher.on[x];

                    let onTypes = [ on ];

                    if (on === '*') {

                        onTypes = ['insert', 'update', 'delete'];

                    }

                    for (let j = 0; j < onTypes.length; j++) {

                        let onT = onTypes[j];
                        if (typeof this.watchers[watcher.table][onT] === 'undefined') {

                            this.watchers[watcher.table][onT] = {};

                        }

                        this.watchers[watcher.table][onT][actionHash] = watcher.action;

                    }

                }

            }

        } catch (error) {

            throw this.error.handle({ error, caller: '_setWatchers' });

        }

    }

    /**
     * A private method for updating existing schema's tables.
     * @access private
     * @param {object} config - available options
     * @returns {Promise<object>} returns an object
     */
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

    /**
     * A private method for applying foriegn keys to schema's tables.
     * @access private
     * @param {object} config - available options
     * @returns {Promise<string>} returns an object
     */
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

    /**
     * A private method for creating schema's tables.
     * @access private
     * @param {object} config - available options
     * @returns {Promise<object>} returns an object
     */
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

    /**
     * A private method for defining the schema's table columns.
     * @access private
     * @param {object} config - available options
     * @returns {Promise<object>} returns an object
     */
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

/**
 * Exposes Base Class.
 * @static
 * @returns {Base} Base class
 */
Layerize.Base = Base;

/**
 * Exposes Crud Class.
 * @static
 * @returns {Crud} Crud class
 */
Layerize.Crud = Crud;

/**
 * Exposes Layers Class.
 * @static
 * @returns {Layers} Layers class
 */
Layerize.Layers = Layers;

/**
 * Exposes Schemas Class.
 * @static
 * @returns {Schemas} Schemas class
 */
Layerize.Schemas = Schemas;

/**
 * Exposes Tables Class.
 * @static
 * @returns {Tables} Tables class
 */
Layerize.Tables = Tables;

/**
 * Exposes Permissions Class.
 * @static
 * @returns {Permissions} Permissions class
 */
Layerize.Permissions = Permissions;

/**
 * Exposes utils.
 * @static
 * @returns {object} utils object
 */
Layerize.utils = utils;

module.exports = Layerize;
