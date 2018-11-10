'use strict';

const debug = require('debug')('layerize');
const knex = require('knex');
const elasticSearch = require('elasticsearch');
const { Layers, Schemas } = require('./services');
const { jsonValidator, errors, redis, objects } = require('./utils');

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
        this.dbSchemas = {};
        this.cache = redis.cache;
        this.es = null;
        this.db = null;
        this.layerizeSchemaName = layerizeSchemaName;

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
             * load system table schemas
             * */
            let schemas = new Schemas();
            this.systemSchemas = await schemas.load('./lib/data/schemas/**/*.json');

            /**
             * load application table schemas
             * */
            this.schemas = await schemas.load(this.schemaPattern);

            /**
             * load current list of database schemas
             * */
            await this.loadSchemaListFromDB();

            /**
             * install layerizeCore, if it does not exist
             * */
            await this.install({ layerizeCore: true });

            // TODO: check to see if data layer updates need

        } catch (error) {

            throw this.error.handle({ error, caller: 'connect' });

        }

    }

    layers ({ schema, realTimeTransactions = this.realTimeTransactions } = {}) {

        this.debug('layers()');
        try {

            if (typeof this.dbSchemas[schema] === 'undefined') {

                throw new Error(`schema '${schema}' does not exist in the database`);

            }

            return new Layers({
                name: schema,
                schemas: this.schemas,
                database: this.db,
                cache: this.cache,
                search: this.es,
                validator: jsonValidator,
                realTimeTransactions,
                cacheExpireSeconds: this.cacheExpireSeconds
            });

        } catch (error) {

            throw this.error.handle({ error, caller: 'layers' });

        }

    }

    async install ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('install');
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

            await this._buildTables({ schemaName, schemas });

        } catch (error) {

            throw this.error.handle({ error, caller: 'install' });

        }

    }

    async uninstall ({ schemaName = '', layerizeCore = false } = {}) {

        this.debug('uninstall');
        try {

            if (layerizeCore) {

                schemaName = this.layerizeSchemaName;

            }

            if (schemaName === '') {

                throw new Error('passing a schemaName is required when using uninstall({ schemaName })');

            }

            await this.db.schema.raw(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);

            await this.loadSchemaListFromDB();

            return true;

        } catch (error) {

            throw this.error.handle({ error, caller: 'uninstall' });

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

    async _buildTables ({ schemaName = 'public', schemas = this.schemas } = {}) {

        this.debug(`_buildTables({ schemaName: ${schemaName}})`);
        try {

            if (typeof this.dbSchemas[schemaName] === 'undefined') {

                let tables = Object.keys(schemas.layerize);
                for (let i = 0; i < tables.length; i++) {

                    let table = schemas.layerize[tables[i]];
                    let columns = Object.keys(table.columns);
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

                    await this.db.schema.raw(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`).withSchema(schemaName).createTable(table.name, (tbl) => {

                        for (let i = 0; i < columns.length; i++) {

                            let column = table.columns[columns[i]];

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

                    this.debug('complete');

                }

                await this.loadSchemaListFromDB();

            }

            return true;

        } catch (error) {

            throw this.error.handle({ error, caller: '_buildTables' });

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

}

module.exports = Layerize;
