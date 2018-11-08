'use strict';

const debug = require('debug')('layerize');
const knex = require('knex');
const elasticSearch = require('elasticsearch');
const { Layers, Schemas } = require('./services');
const { jsonValidator, errors, redis, objects } = require('./utils');

class Layerize {

    constructor ({ schemas = '', keywords = '', formats = '', cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false } = {}) {

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

    }

    async initiate ({ setup = false } = {}) {

        this.debug('initiate()');
        try {

            this.db = knex({
                client: 'pg',
                connection: {
                    host: 'localhost',
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

            redis.init(6379, 'localhost');

            this.es = new elasticSearch.Client({ host: 'http://localhost:9200/' });
            this.es.ping({
                requestTimeout: 3000
            }, (err) => {

                if (err) {

                    throw new Error('can not connect to elastic search');

                }

            });

            let schemas = new Schemas();
            this.systemSchemas = await schemas.load('./lib/data/schemas/**/*.json');
            this.debug(' -- System schemas loaded');

            // load current list of database schemas
            let schemaList = await this.loadSchemaListFromDB();

            if (setup) {

                await this.buildTables({ schemaName: 'layerize', schemas: this.systemSchemas });

                // this.debug(await this.db.schema.raw('SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema = \'layerize\';'));

            } else {

                if (typeof schemaList.layerize === 'undefined') {

                    throw new Error('layerizeJS core tables have not been set, try again with the \'setup\' flag true. initiate ({ setup: true })');

                }

            }

            // load schemas

            // check to see if data layer updates need

            this.schemas = await schemas.load(this.schemaPattern);
            this.debug(' -- Schemas loaded');
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

            // let layers = this.layers({ schema: 'public2' });
            // let users = await layers.search('users');

            // let layers = this.layers({ schema: 'public2' });
            // let users = await layers.search('users', { fields: 'id', sort: 'username', filter: ['archived:false', 'first_name:John3&&system_keys:![{"key":"2"}]'] });
            // this.debug(users);

            // await layers.clearTableCache('users');

            // user.username = 'pickler';
            // this.debug(await layers.update('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', user, { returnRecord: true }));

            // user = await layers.patch('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd', { first_name: 'John3' }, { returnRecord: true });
            // this.debug(user);
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

    async loadEnvironment () {

        this.debug('loadEnvironment()');
        try {

            // let exist = await this.db.schema.hasTable(table.hasName);

        } catch (error) {

            throw this.error.handle({ error, caller: 'loadEnvironment' });

        }

    }

    async buildTables ({ schemaName = 'public', schemas = this.schemas } = {}) {

        this.debug(`buildTables({ schemaName: ${schemaName}})`);
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

            }

            return true;

        } catch (error) {

            throw this.error.handle({ error, caller: 'buildTables' });

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

    // search(filter)

    // get(id)

    // update(id || filter)

    // insert()

}

module.exports = Layerize;
