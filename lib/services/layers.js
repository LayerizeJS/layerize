/* eslint-disable consistent-this, no-warning-comments, no-unused-vars */
'use strict';

const debug = require('debug')('layerize:layers');
const extend = require('extend');
const { errors, requestFields, redis, objects, uuid } = require('../utils');
const Permissions = require('./permissions');
const { setLock } = redis;
const { error } = console;

/**
 * @typedef {Object} success
 * @property {boolean} success
 */

/**
 * @typedef {Object} objSQL
 * @property {boolean} success
 */

/**
 * @typedef {Object} permission
 * @property {boolean} granted
 */

/**
 * @typedef {Object} results
 * @property {string} sort
 * @property {number} offset
 * @property {number} total
 * @property {array} items
 */

/**
 * @typedef {Object} layerizeFilter
 * @property {array} raw
 * @property {array} columns
 * @property {array} structure
 * @property {object} query
 * @property {object} native
 * @property {boolean} native.active
 * @property {string} native.type
 * @property {string} native.where
* @property {object} native.body
 */

/**
 * The **Layers class** handles all the data searching, inserting, updating, patching, deleting, replicating and more. It manages each data source (sql based database, elasticsearch and redis caching) to keep data in sync and decides when to pull data from which data source. It is highly optimized to use best practices and focuses on scalibilty and speed.
 */
class Layers {

    /**
     * Create a layer.
     * @param {String} name - schema name of the database.
     */
    constructor ({ layerize, name, database, cache, search, dynamoTables, schemas, validator, cacheExpireSeconds = 60 * 60 * 24 * 7, lockTimeoutMilliseconds = 30 * 1000, realTimeTransactions = false, isTransaction = false, permissions = {} } = {}) {

        if (typeof layerize !== 'object' || layerize === null || typeof layerize === 'undefined') {

            throw new errors.Error({ message: 'A valid instance of Layerize must be passed to the class.' });

        }

        /**
         * Sets the layerize instance
         * @member {layerize}
         * */
        this.layerize = layerize;

        /**
         * Sets the layer to a transaction mode
         * @member {boolean}
         * */
        this.isTransaction = isTransaction;

        /**
         * Name of the schema being used within the layer
         * @member {string}
         * */
        this.schemaName = name;

        /**
         * The initialized knex instance to use
         * @member {knex}
         * */
        this.database = database || layerize.db;

        /**
         * The cache utility to use
         * @member {object}
         * */
        this.cache = cache || layerize.cache;

        /**
         * The initialized elasticsearch instance to use
         * @member {elasticsearch}
         * */
        this.es = search || layerize.es;

        /**
         * The initialized dynamoTable instance to use
         * @member {object}
         * */
        this.dynamoTables = dynamoTables || layerize.dynamoTables;

        /**
         * The schemas object to use
         * @member {schemas}
         * */
        this.schemas = schemas;

        /**
         * The initialized ajv instance to use
         * @member {JsonValidator}
         * */
        this.validator = validator;

        /**
         * If it is in transaction mode is it using layerize transaction or traditional sql
         * @member {boolean}
         * */
        this.realTimeTransactions = realTimeTransactions;

        /**
         * The number of seconds the cache expires in
         * @member {number}
         * */
        this.cacheExpireSeconds = cacheExpireSeconds || layerize.cacheExpireSeconds;

        /**
         * The error utility
         * @member {object}
         * */
        this.error = errors;

        /**
         * The initialized debug instance
         * @member {debug}
         * */
        this.debug = debug;

        /**
         * Holds the statements that will be executed as a transaction
         * @member {array}
         * */
        this.transactions = [];

        /**
         * The number of milliseconds the lock expires in
         * @member {number}
         * */
        this.lockTimeoutMilliseconds = lockTimeoutMilliseconds;

        /**
         * If audit logs are enabled
         * @member {boolean}
         * */
        this.auditLogs = layerize.auditLogs;

        /**
         * The prepend name of the cache key
         * @member {string}
         * */
        this.cacheKey = `LAYERIZE:DATA:${this.schemaName.toUpperCase()}`;

        /**
         * The prepend name of the lock key
         * @member {string}
         * */
        this.lockKey = `LAYERIZE:LOCK:${this.schemaName.toUpperCase()}`;

        /**
         * Holds the active locks that get released when transaction is committed or rolled back
         * @member {object}
         * */
        this.activeLocks = {};

        /**
         * Holds the bulk elasticsearch array that gets processed on commit
         * @member {array}
         * */
        this.esTransaction = [];

        /**
         * A flag for knowing if another layer's transaction was combined in
         * @member {boolean}
         * */
        this.hasBeenCombined = false;

        this.permissions = new Permissions({ layerize, schemaName: name });

        /**
         * Holds read permission object
         * @member {object}
         * */
        this.readPermission = permissions.read || {};

        /**
         * Holds create permission object
         * @member {object}
         * */
        this.createPermission = permissions.create || {};

        /**
         * Holds update permission object
         * @member {object}
         * */
        this.updatePermission = permissions.update || {};

        /**
         * Holds delete permission object
         * @member {object}
         * */
        this.deletePermission = permissions.delete || {};

        /**
         * A instance of the 'database' member that has been initiated with the schemaName
         * @member {knex}
         * */
        this.dbSchema = this.database.withSchema(this.schemaName).clone();

    }

    /**
     * Clears all tables caches
     * @example
     * let response = await layer.clearAllTablesCache();
     * @returns {Promise<success>} { success: true }
     * @throws {Error} "clearAllTablesCache() can not be called inside a transaction"
     */
    async clearAllTablesCache () {

        this.debug('clearAllTablesCache()');
        try {

            if (this.isTransaction === true) {

                throw new errors.Error({ message: 'clearAllTablesCache() can not be called inside a transaction' });

            }

            const cacheKey = this.cache.key(this.cacheKey, '*');
            return await this.cache.deleteByPattern(cacheKey);

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearAllTablesCache' });

        }

    }

    /**
     * Clears a single record cache
     * @param {string} table - name of the table
     * @param {string} key - primary key of the record
     * @example
     * let response = await layer.clearRecordCache('users', 'a99f0cea-c3df-4619-b023-8c71fee3a9cd');
     * @returns {Promise<success>} { success: true }
     * @throws {Error} "clearRecordCache() can not be called inside a transaction"
     */
    async clearRecordCache (table = '', key = '') {

        this.debug(`clearRecordCache(${table})`);
        try {

            if (this.isTransaction === true) {

                throw new errors.Error({ message: 'clearRecordCache() can not be called inside a transaction' });

            }

            if (table === '') {

                throw new errors.Error({ message: 'passing a table name is required when using clearRecordCache(table, key)' });

            }

            if (key === '') {

                throw new errors.Error({ message: 'passing a record key is required when using clearRecordCache(table, key)' });

            }

            const cacheKey = this.cache.key(this.cacheKey, table, key);
            await this.cache.clear(cacheKey);

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearRecordCache' });

        }

    }

    /**
     * Clears a single tables cache
     * @param {string} table - name of the table
     * @example
     * let transactions = await layer.clearTableCache('users');
     * @returns {Promise<success>} { success: true }
     * @throws {Error} "clearTableCache() can not be called inside a transaction"
     */
    async clearTableCache (table = '') {

        this.debug(`clearTableCache(${table})`);
        try {

            if (this.isTransaction === true) {

                throw new errors.Error({ message: 'clearTableCache() can not be called inside a transaction' });

            }

            if (table === '') {

                throw new errors.Error({ message: 'passing a table name is required when using clearTableCache(table)' });

            }

            const cacheKey = this.cache.key(this.cacheKey, table, '*');
            await this.cache.deleteByPattern(cacheKey);

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearTableCache' });

        }

    }

    /**
     * Combines the transactions of from two different layer instances
     * @param {Layer} layer - another layer instance that is being combined
     * @example
     * let response = layer.combine(otherLayer);
     * @returns {array} [] - array of transactions
     * @throws {Error} "combine() can only be called inside a transaction"
     * @throws {Error} "combine() can only be used when 'realTimeTransactions' is set to 'false'"
     */
    combine (layer) {

        this.debug('combine');
        try {

            if (this.isTransaction === false) {

                throw new errors.Error({ message: 'combine() can only be called inside a transaction' });

            }

            if (this.realTimeTransactions === true) {

                throw new errors.Error({ message: 'combine() can only be used when \'realTimeTransactions\' is set to \'false\'' });

            }

            this.activeLocks = extend(true, this.activeLocks, layer.activeLocks);
            this.transactions = this.transactions.concat(layer.transactions);

            this.hasBeenCombined = true;

            layer.transactions = [];
            layer.activeLocks = {};

            return this.transactions;

        } catch (error) {

            throw this.error.handle({ error, caller: 'combine' });

        }

    }

    /**
     * Commits the transactions that in the layer
     * @param {object=} options - available options
     * @param {object=} [options.transactions=[]] - array of transactions to be combined in
     * @param {boolean=} [options.autoRollback=true] - auto rollback if commit fails
     * @returns {Promise<array>} { results }
     * @example
     * let response = await layer.commit();
     * @throws {Error} "commit() can only be called inside a transaction"
     */
    async commit ({ transactions = [], autoRollback = true } = {}) {

        this.debug('commit()');
        try {

            if (this.isTransaction === false) {

                throw new errors.Error({ message: 'commit() can only be called inside a transaction' });

            }

            if (transactions.length > 0) {

                this.transactions = this.transactions.concat(transactions);

            }

            let results = [];

            if (this.transactions.length > 0) {

                let trx = await this._createTransaction();
                let cacheToDelete = [];
                let populateToES = [];
                let populateToCache = [];
                let removeFromES = [];
                let auditLogsToSave = [];
                let watcherTables = [];

                // TODO: cleanup all the repeatative result.rows.map
                for (let i = 0; i < this.transactions.length; i++) {

                    let statement = this.transactions[i];

                    let result = await this.database.raw(statement.sql, statement.bindings).transacting(trx);

                    if (typeof statement !== 'undefined') {

                        if (statement.populateToES) {

                            let keys = [];
                            let items;
                            if (typeof statement.keys !== 'undefined') {

                                keys = statement.keys;

                            } else {

                                keys = result.rows.map(row => row[statement.primaryKey]);
                                items = result.rows;

                            }

                            populateToES.push({
                                table: statement.table,
                                primaryKey: statement.primaryKey,
                                keys,
                                items
                            });

                        }

                        if (statement.populateToCache) {

                            populateToCache.push({
                                table: statement.table,
                                primaryKey: statement.primaryKey,
                                keys: result.rows.map(row => row[statement.primaryKey]),
                                items: result.rows
                            });

                        }

                        if (statement.removeFromES) {

                            removeFromES.push({
                                table: statement.table,
                                primaryKey: statement.primaryKey,
                                keys: result.rows.map(row => row[statement.primaryKey]),
                                items: result.rows
                            });

                        }

                        if (statement.removeFromCache) {

                            if (typeof statement.cache === 'undefined') {

                                statement.cache = [];

                            }

                            for (let x = 0; x < result.rows.length; x++) {

                                let key = result.rows[x][statement.primaryKey];
                                statement.cache.push(this.cache.key(this.cacheKey, statement.table, key));

                            }

                        }

                        if (Array.isArray(statement.cache) && statement.cache.length > 0) {

                            cacheToDelete = cacheToDelete.concat(statement.cache);

                        }

                        if (Array.isArray(statement.auditLogs) && statement.auditLogs.length > 0) {

                            auditLogsToSave = auditLogsToSave.concat(statement.auditLogs);

                        }

                        if (typeof statement.table !== 'undefined' && statement.table !== '' &&
                            typeof statement.type !== 'undefined' && statement.type !== '' &&
                            Array.isArray(statement.keys) && statement.keys.length > 0) {

                            let keys = [];
                            if (typeof statement.keys !== 'undefined') {

                                keys = statement.keys;

                            } else {

                                statement.keys = keys = result.rows.map(row => row[statement.primaryKey]);

                            }

                            watcherTables.push({ table: statement.table, on: statement.type, keys });

                        }

                    }

                    results.push(result);

                }

                try {

                    await trx.commit();

                    if (cacheToDelete.length > 0) {

                        await this.cache.clear(...cacheToDelete);

                    }

                    if (removeFromES.length > 0) {

                        for (let i = 0; i < removeFromES.length; i++) {

                            let obj = removeFromES[i];

                            for (let x = 0; x < obj.keys.length; x++) {

                                let key = obj.keys[x];
                                this.esTransaction.push({ delete: { _index: `${this.schemaName}~${obj.table}`, _type: 'index', _id: key } });

                            }

                        }

                    }

                    if (populateToES.length > 0) {

                        for (let i = 0; i < populateToES.length; i++) {

                            let obj = populateToES[i];

                            if (typeof obj.items === 'undefined') {

                                obj.items = await this.getMany(obj.table, obj.keys);

                            }

                            for (let x = 0; x < obj.items.length; x++) {

                                let result = obj.items[x];
                                this.esTransaction.push({ index: { _index: `${this.schemaName}~${obj.table}`, _type: 'index', _id: result[obj.primaryKey] } });
                                this.esTransaction.push(result);

                            }

                        }

                    }

                    if (populateToCache.length > 0) {

                        let cachebulk = [];
                        for (let i = 0; i < populateToCache.length; i++) {

                            let obj = populateToCache[i];

                            for (let x = 0; x < obj.items.length; x++) {

                                let result = obj.items[x];

                                cachebulk.push({
                                    key: this.cache.key(this.cacheKey, obj.table, result[obj.primaryKey]),
                                    value: result
                                });

                            }

                        }

                        if (cachebulk.length > 0) {

                            await this.cache.mset(cachebulk, this.cacheExpireSeconds);

                        }

                    }

                    if (this.esTransaction.length > 0) {

                        await this.es.bulk({
                            body: this.esTransaction
                        });

                    }

                    if (auditLogsToSave.length > 0) {

                        await this._saveAuditLogs({ logs: auditLogsToSave });

                    }

                    await this._runWatcher({ tables: watcherTables });

                } catch (e) {

                    if (autoRollback) {

                        await trx.rollback();

                    }

                    throw e;

                }

            }

            this.transactions = [];

            Object.keys(this.activeLocks).map((key) => {

                return this.activeLocks[key].unlock();

            });

            this.activeLocks = {};

            return { results };

        } catch (error) {

            throw this.error.handle({ error, caller: 'commit' });

        }

    }

    /**
     * Gets the record count of a table from the data layer.
     * @param {string} table - name of the table
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {string|array|object} [options.filter=''] - layerize filter syntax
     * @param {permission} [options.readPermission={}] - permission object
     * @param {boolean=} [options.estimated=false] - when false it uses reltuples::bigint
     * @example
     * let totalCount = await layer.count('users');
     * @returns {Promise<number>} A number representing the count. Example: 25
     */
    async count (table = '', { fields = '*', filter = '', estimated = false, readPermission = this.readPermission } = {}) {

        this.debug(`count(${table})`);
        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(readPermission);

            let statement;

            if (estimated === false) {

                statement = this.dbSchema.clone().table(table).count(fields);
                ({ statement } = await this._applyFilters(statement, { table, filter, authorization }));

            } else {

                statement = this.database.raw(`SELECT reltuples::bigint AS count FROM pg_class WHERE oid = '${this.schemaName}.${table}'::regclass;`);

            }

            let results = await statement;
            let count = 0;

            if (Array.isArray(results)) {

                count = results[0].count;

            } else {

                count = results.rows[0].count;

            }

            return parseInt(count, 10);

        } catch (error) {

            throw this.error.handle({ error, caller: 'count' });

        }

    }

    /**
     * Deletes a single records from the data layer
     * @param {string} table - name of the table
     * @param {string|array} key - primary key of the record
     * @param {object=} options - available options
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If the instance of the layer IS a transaction, then it will return objSQL.
     * @returns {Promise<success>} If the instance of the layer IS NOT a transaction, then it will return a success object.
     */
    async delete (table = '', key = '', { deletePermission = this.deletePermission } = {}) {

        this.debug(`delete(${table})`);
        try {

            let keys = key;
            if (!Array.isArray(key)) {

                keys = [ key ];

            }

            return await this.deleteMany(table, keys, { deletePermission });

        } catch (error) {

            throw this.error.handle({ error, caller: 'delete' });

        }

    }

    /**
     * Deletes a single or mulitple records from the data layer that is meets the filter restraint
     * @param {string} table - name of the table
     * @param {string|array|object} filter - layerize filter syntax
     * @param {object=} options - available options
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If the instance of the layer IS a transaction, then it will return objSQL.
     * @returns {Promise<success>} If the instance of the layer IS NOT a transaction, then it will return a success object.
     */
    async deleteByFilter (table = '', filter = '', { deletePermission = this.deletePermission } = {}) {

        this.debug(`deleteByFilter(${table})`);
        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(deletePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            let statement = this.dbSchema.clone().table(table);

            ({ statement } = await this._applyFilters(statement, { table, filter, authorization }));
            statement.delete();
            statement.returning(schema.primaryKey);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.table = table;
                objSQL.primaryKey = schema.primaryKey;
                objSQL.removeFromES = true;
                objSQL.removeFromCache = true;
                objSQL.type = 'delete';
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let keys = await statement;
                let esTransaction = [];
                let cacheKeys = [];

                for (let i = 0; i < keys.length; i++) {

                    let key = keys[i];

                    cacheKeys.push(this.cache.key(this.cacheKey, table, key));

                    if (schema.esEnabled) {

                        esTransaction.push({ delete: { _index: `${this.schemaName}~${table}`, _type: 'index', _id: key } });

                    }

                }

                if (cacheKeys.length > 0) {

                    await this.cache.clear(...cacheKeys);

                }

                if (esTransaction.length > 0) {

                    let esResponse = await this.es.bulk({ body: esTransaction });

                    if (esResponse.errors) {

                        error(JSON.stringify(esResponse));

                    }

                }

                await this._runWatcher({ tables: [ { table, on: 'delete', keys } ] });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'deleteByFilter' });

        }

    }

    /**
     * Deletes a many records by key from the data layer
     * @param {string} table - name of the table
     * @param {array} keys - primary key of the record
     * @param {object=} options - available options
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If the instance of the layer IS a transaction, then it will return objSQL.
     * @returns {Promise<success>} If the instance of the layer IS NOT a transaction, then it will return a success object.
     */
    async deleteMany (table = '', keys = [], { deletePermission = this.deletePermission } = {}) {

        this.debug(`deleteMany(${table})`);
        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(deletePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            // TODO: if authorization has conditions then do a getMany and check if all files qualify to be deleted

            let statement = this.dbSchema.clone().table(table).delete().whereIn(schema.primaryKey, keys);

            let cacheKeys = [];
            let esTransaction = [];

            for (let i = 0; i < keys.length; i++) {

                let key = keys[i];
                cacheKeys.push(this.cache.key(this.cacheKey, table, key));
                esTransaction.push({ delete: { _index: `${this.schemaName}~${table}`, _type: 'index', _id: key } });

            }

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.table = table;
                objSQL.type = 'delete';
                objSQL.cache = cacheKeys;
                this.transactions.push(objSQL);

                if (schema.esEnabled) {

                    this.esTransaction = this.esTransaction.concat(esTransaction);

                }

                return objSQL;

            } else {

                let result = await statement;

                await this.cache.clear(...cacheKeys);

                if (schema.esEnabled) {

                    await this.es.bulk({ body: esTransaction });

                }

                await this._runWatcher({ tables: [ { table, on: 'delete', keys } ] });

                return result;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'deleteMany' });

        }

    }

    /**
     * Gets a single record from the data layer
     * @param {string} table - name of the table
     * @param {string|array} key - primary key of the record
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {boolean} [options.forUpdate=false] - will lock record for updating
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<object>} return the record object
     */
    async get (table = '', key = '', { fields = '*', includes = '', forUpdate = false, readPermission = this.readPermission } = {}) {

        this.debug(`get(${table})`, { includes });
        try {

            let keys = key;
            if (!Array.isArray(key)) {

                keys = [ key ];

            }

            // await this.getAuditLog(table, key);

            let results = await this.getMany(table, keys, { fields, includes, forUpdate, readPermission });

            if (!Array.isArray(key)) {

                return results[0];

            } else {

                return results;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

        }

    }

    /**
     * Gets a single record's audit log
     * @param {string} table - name of the table
     * @param {string|array} key - primary key of the record
     * @returns {Promise<array>} return the record's audit logs
     */
    async getAuditLog (table = '', key = '') {

        this.debug(`getAuditLog(${table})`);
        try {

            const table = this.dynamoTables.table('audit-logs', this.schemaName);
            let results = await table.query(key).exec().promise();
            return results[0].Items.map(item => item.attrs);

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

        }

    }

    /**
     * Gets a many records by key from the data layer
     * @param {string} table - name of the table
     * @param {array} keys - primary keys of the records being requests
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {boolean} [options.forUpdate=false] - will lock record for updating
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<array>} return an array of record objects
     */
    async getMany (table = '', keys = [], { fields = '*', includes = '', forUpdate = false, readPermission = this.readPermission } = {}) {

        this.debug(`getMany(${table})`);

        let activeLocks = {};

        try {

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(readPermission);

            if (forUpdate) {

                if (this.isTransaction === false) {

                    throw new errors.Error({ message: 'forUpdate flag can only be used inside a transaction' });

                }

                // if (typeof this.activeLocks[key] === 'undefined') {

                //     currentRecord = await layer.get(table, key, { forUpdate: true });

                // } else {

                //     currentRecord = this.activeLocks[key].data;

                // }

                for (let i = 0; i < keys.length; i++) {

                    let key = keys[i];
                    // console.log('locking', key);

                    if (typeof this.activeLocks[key] === 'undefined') {

                        activeLocks[key] = { key, data: {}, unlock: await setLock(this.cache.key(this.lockKey, key), this.lockTimeoutMilliseconds) };

                    }

                }

            }

            let fromActiveLocks = [];
            let cacheKeys = [];
            for (let i = 0; i < keys.length; i++) {

                let key = keys[i];
                if (typeof this.activeLocks[key] !== 'undefined') {

                    fromActiveLocks.push(this.activeLocks[key].data);

                } else {

                    cacheKeys.push(this.cache.key(this.cacheKey, table, key));

                }

            }

            let results = fromActiveLocks;

            // See if caching layer has records
            if (cacheKeys.length > 0) {

                results = results.concat(await this.cache.get(cacheKeys));

            }

            let notFoundInCache = [];
            let notFoundMap = {};

            for (let i = 0; i < results.length; i++) {

                let result = results[i];
                if (result === null) {

                    // add to list for pulling from db
                    notFoundInCache.push(keys[i]);

                    // create map to easily plug back into results, records are returned in same order as received
                    notFoundMap[keys[i]] = i;

                }

            }

            // if caching layer does not have record the pull it from the data source
            if (notFoundInCache.length > 0) {

                this.debug('using db to pull records into cache');

                let statement = this.dbSchema.clone().table(table).whereIn(schema.primaryKey, notFoundInCache);

                let dbResults = await statement;

                if (dbResults.length === 0) {

                    throw new errors.Error({ message: `Records '${notFoundInCache.join(',')}' could not be found in '${table}' table.` });

                } else {

                    let cachebulk = [];
                    for (let i = 0; i < dbResults.length; i++) {

                        let item = dbResults[i];

                        results[notFoundMap[item[schema.primaryKey]]] = item;

                        cachebulk.push({
                            key: this.cache.key(this.cacheKey, table, item[schema.primaryKey]),
                            value: item
                        });

                    }

                    await this.cache.mset(cachebulk, this.cacheExpireSeconds);

                }

                let stillNotFound = [];
                for (let i = 0; i < results.length; i++) {

                    let result = results[i];
                    if (result === null) {

                        stillNotFound.push(keys[i]);

                    }

                }

                if (stillNotFound.length > 0) {

                    throw new errors.Error({ message: `Records '${stillNotFound.join(',')}' could not be found in '${table}' table.` });

                }

            } else {

                this.debug('all records retrieved from cache');

            }

            results = await this.permissions.authorizedData(authorization, results);

            if (forUpdate) {

                for (let i = 0; i < results.length; i++) {

                    let key = results[i][schema.primaryKey];
                    if (typeof activeLocks[key] !== 'undefined') {

                        activeLocks[key].data = extend(true, {}, results[i]);

                    }

                }

            }

            ({ results } = await this._includes({ table, results, includes }));

            let response = requestFields.parse(results, { stacked: [...authorization.properties, fields] });

            this.activeLocks = extend(true, this.activeLocks, activeLocks);

            return response;

        } catch (error) {

            if (this.isTransaction) {

                await this.rollback();

            }
            throw this.error.handle({ error, caller: 'getMany' });

        }

    }

    /**
     * Inserts a single or mulitple records into the data layer
     * @param {string} table - name of the table
     * @param {object|array} data - the data of the records being inserted
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecord=false] - will return records after inserting
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If the instance of the layer IS a transaction, then it will return objSQL.
     * @returns {Promise<object>} If the instance of the layer IS NOT a transaction and returnRecord is 'true', then it will return the record object.
     * @returns {Promise<success>} If the instance of the layer IS NOT a transaction and returnRecord is 'false', then it will return a success object.
     * @throws {Error} The 'table' parameter is required when doing an insert() or insertMany().
     * @throws {Error} The 'data' parameter is required when doing an insert() or insertMany().
     */
    async insert (table = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '*', includes = '', createPermission = this.createPermission } = {}) {

        this.debug(`insert(${table})`, returnRecord);
        try {

            if (!Array.isArray(data)) {

                data = [ data ];

            }

            let results = await this.insertMany(table, data, { returnRecords: returnRecord, ignoreReadOnly, fields, includes, createPermission });

            if (returnRecord) {

                return results[0];

            } else {

                return results;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'insert' });

        }

    }

    /**
     * Inserts a mulitple records into the data layer
     * @param {string} table - name of the table
     * @param {array} data - the data of the records being inserted
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<object>} If layer IS NOT a transaction and returnRecord is 'true', then it will return the reocrd object
     * @returns {Promise<success>} If layer IS NOT a transaction and returnRecord is 'false', then it will return a success object
     * @throws {Error} The 'table' parameter is required when doing an insert() or insertMany().
     * @throws {Error} The 'data' parameter is required when doing an insert() or insertMany().
     */
    async insertMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '*', includes = '', createPermission = this.createPermission } = {}) {

        this.debug(`insertMany(${table})`);
        try {

            if (table === '') {

                throw new errors.Error({ message: 'The \'table\' parameter is required when doing an insert() or insertMany().' });

            }

            if (data.length === 0) {

                throw new errors.Error({ message: 'Data parameter is required when doing an insert() or insertMany()' });

            }

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(createPermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            for (let i = 0; i < data.length; i++) {

                await this._validate({ table, data: data[i] });

                ({ data: data[i] } = await this.__convertForDatabase({ type: 'insert', table, data: data[i], ignoreReadOnly, schema }));

            }

            let statement = this.dbSchema.clone().table(table).insert(data);

            if (this.isTransaction) {

                let objSQL;

                if (schema.esEnabled) {

                    statement.returning('*');

                    objSQL = statement.toSQL();
                    objSQL.primaryKey = schema.primaryKey;
                    objSQL.populateToES = true;
                    objSQL.populateToCache = true;

                } else {

                    objSQL = statement.toSQL();

                }

                objSQL.table = table;
                objSQL.type = 'insert';

                this.transactions.push(objSQL);

                return objSQL;

            } else {

                return await this.__nonTransactionESCacheProcessing({ table, schema, statement, returnRecords, fields, includes, authorization });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'insertMany' });

        }

    }

    /**
     * Patches a single partial records into the data layer
     * @param {string} table - name of the table
     * @param {string} key - primary key of the record
     * @param {object} data - the data of the record being patched
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<object>} If layer IS NOT a transaction and returnRecord is 'true', then it will return the reocrd object
     * @returns {Promise<success>} If layer IS NOT a transaction and returnRecord is 'false', then it will return a success object
     * @throws {Error} The 'table' parameter is required when doing an patch().
     * @throws {Error} The 'key' parameter is required when doing an patch().
     * @throws {Error} The 'data' parameter is required when doing an patch().
     */
    async patch (table = '', key = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '*', includes = '', updatePermission = this.updatePermission } = {}) {

        this.debug(`patch(${table})`);

        let layer = this;

        // a flag for knowing if we are using our own temporary transaction or not
        let internalTransaction = false;

        try {

            if (table === '') {

                throw new errors.Error({ message: 'The \'table\' parameter is required when doing an patch().' });

            }

            if (key === '') {

                throw new errors.Error({ message: 'The \'key\' parameter is required when doing an patch().' });

            }

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(updatePermission);

            if (Object.keys(data).length === 0) {

                throw new errors.Error({ message: 'The \'data\' parameter is required when doing an patch().' });

            }

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            // if not a transaction, then create a temporary transaction layer to use for this update
            if (!this.isTransaction) {

                layer = this.transaction();
                internalTransaction = true;

            }

            // permission free because we want the full record
            let currentRecord = await layer.get(table, key, { forUpdate: true, readPermission: {} });

            // combine patch with current record
            ({ data } = this._mergePatch({ currentRecord, patch: data, schema }));

            // validate against json schema and remove any fields that are readonly, unless ignored
            await this._validate({ table, data });

            let auditLogs = [];

            // this could set to false if there is no differences in what is requested to save
            let saveUpdates = true;

            // check if conditions need evaluating or, if so then make sure it is populated
            if (authorization.conditions.length > 0 || this.auditLogs) {

                ({ item: data, saveUpdates, auditLogs } = await this.__authorizingConditions({ authorization, currentRecords: [ currentRecord ], schema, key, item: data, table, auditLogs, saveUpdates }));

            }

            ({ data } = await this.__convertForDatabase({ table, data, ignoreReadOnly, schema }));

            this.debug('auditLogs', auditLogs);

            if (saveUpdates) {

                let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(data);

                const cacheKey = this.cache.key(this.cacheKey, table, key);

                if (this.isTransaction) {

                    let objSQL;

                    if (schema.esEnabled) {

                        statement.returning('*');

                        objSQL = statement.toSQL();
                        objSQL.primaryKey = schema.primaryKey;
                        objSQL.populateToES = true;
                        objSQL.populateToCache = true;

                    } else {

                        objSQL = statement.toSQL();
                        objSQL.cache = [ cacheKey ];

                    }

                    objSQL.keys = [ key ];
                    objSQL.auditLogs = auditLogs;
                    objSQL.table = table;
                    objSQL.type = 'update';
                    this.transactions.push(objSQL);
                    return objSQL;

                } else {

                    let results = await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys: [ key ], schema, statement, returnRecords: returnRecord, fields, includes, authorization, layer });
                    return results[0];

                }

            } else { // no save was needed

                if (this.isTransaction) {

                    return null;

                } else {

                    // cancel transaction and unlock records as no changes need
                    await layer.rollback();

                    if (returnRecord) {

                        return currentRecord;

                    }

                    return { success: true };

                }

            }

        } catch (error) {

            if (internalTransaction && layer.isTransaction) {

                await layer.rollback();

            }

            throw this.error.handle({ error, caller: 'patch' });

        }

    }

    /**
     * Patches a single or mulitple partial records from the data layer that is meets the filter restraint
     * @param {string} table - name of the table
     * @param {string|array|object} filter - layerize filter syntax
     * @param {object} data - the data being patched
     * @param {object=} options - available options
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<success>} If layer IS NOT a transaction, then it will return a success object
     */
    async patchByFilter (table = '', filter = '', data = {}, { ignoreReadOnly = [], updatePermission = this.updatePermission } = {}) {

        this.debug(`patchByFilter(${table})`);
        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(updatePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            let narrowedSchema = this._narrowedSchema({ schema, properties: Object.keys(data) });

            await this._validate({ table, data, schema: narrowedSchema });

            ({ data } = await this.__convertForDatabase({ table, data, ignoreReadOnly, schema }));

            let statement = this.dbSchema.clone().table(table);
            ({ statement } = await this._applyFilters(statement, { table, filter, authorization }));
            statement.update(data).returning('*');

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.table = table;
                objSQL.primaryKey = schema.primaryKey;
                objSQL.populateToCache = true;

                if (schema.esEnabled) {

                    objSQL.populateToES = true;

                }

                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let items = await statement;

                let cachebulk = [];
                if (schema.esEnabled) {

                    let esTransaction = [];

                    for (let x = 0; x < items.length; x++) {

                        let item = items[x];
                        esTransaction.push({ index: { _index: `${this.schemaName}~${table}`, _type: 'index', _id: item[schema.primaryKey] } });
                        esTransaction.push(item);

                        cachebulk.push({
                            key: this.cache.key(this.cacheKey, table, item[schema.primaryKey]),
                            value: item
                        });

                    }

                    if (esTransaction.length > 0) {

                        let esResponse = await this.es.bulk({ body: esTransaction });

                        if (esResponse.errors) {

                            error(JSON.stringify(esResponse));

                        }

                    }

                } else {

                    for (let x = 0; x < items.length; x++) {

                        let item = items[x];
                        cachebulk.push({
                            key: this.cache.key(this.cacheKey, table, item[schema.primaryKey]),
                            value: item
                        });

                    }

                }

                if (cachebulk.length > 0) {

                    await this.cache.mset(cachebulk, this.cacheExpireSeconds);

                }

                return { succes: true };

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'patchByFilter' });

        }

    }

    /**
     * Patches many partial records by key into the data layer
     * @param {string} table - name of the table
     * @param {array} data - the data being patched
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<array>} If layer IS NOT a transaction and returnRecords is 'true', then it will return an array of reocrd objects
     * @returns {Promise<success>} If layer IS NOT a transaction and returnRecords is 'false', then it will return a success object
     */
    async patchMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '*', includes = '', updatePermission = this.updatePermission } = {}) {

        this.debug(`patchMany(${table})`);
        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(updatePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            // TODO: if authorization has conditions then do a getMany and check if all files qualify to be patched

            /**
             * 1. loop through each record
             *      a. make sure the primary key exists, if not throw error
             *      b. get all object keys and put in alphebtical order
             *      c. hash object key array and compare to previous mapHashes,
             *         if hash is not found then create new narrowedSchema and store in mapHash
             *      d. validate data against narrowedSchema
             *      e. return error as soon as one is found
             *      f. update timestamps
             *      g. build seperate update sql statement for each
             *      h. get cache key
             * 2. create transaction if not part of one
             */

            let keys = [];
            let statements = [];
            let schemaMap = {};
            for (let i = 0; i < data.length; i++) {

                let item = data[i];

                // make sure the primary key exists, if not throw error
                if (typeof item[schema.primaryKey] === 'undefined') {

                    throw new errors.Error({ message: 'When using patchMany(), each object in the data array must contain the primary key.' });

                }

                let key = item[schema.primaryKey];

                // storing key for later populating ES
                keys.push(key);

                // get all object keys and put in alphebtical order
                let properties = Object.keys(item).sort();
                let hash = objects.hash(properties);

                // hash object key array and compare to previous mapHashes, if hash is not found then create new narrowedSchema and store in mapHash
                if (typeof schemaMap[hash] === 'undefined') {

                    schemaMap[hash] = this._narrowedSchema({ schema, properties });

                }

                // validate data against narrowedSchema, return error as soon as one is found
                await this._validate({ table, data: item, schema: schemaMap[hash] });

                ({ data: item } = await this.__convertForDatabase({ table, data: item, ignoreReadOnly, schema }));

                // build seperate update sql statement for each
                let objSQL = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(item).toSQL();

                // get cache key
                let cacheKey = await this.cache.key(this.cacheKey, table, key);

                objSQL.cache = [ cacheKey ];
                statements.push(objSQL);

            }

            if (this.isTransaction) {

                statements[0].table = table;
                statements[0].type = 'update';
                statements[0].keys = keys;

                if (schema.esEnabled) {

                    // attach to first record
                    statements[0].populateToES = true;

                }

                this.transactions = this.transactions.concat(statements);
                return statements;

            } else {

                return await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys, schema, statement: statements, returnRecords, fields, includes, authorization });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'patchMany' });

        }

    }

    /**
     * Access to the underlining cache service
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async rawCache () {

        this.debug('__rawCache');
        try {

            //

        } catch (error) {

            throw this.error.handle({ error, caller: '__rawCache' });

        }

    }

    /**
     * Access to the underlining database service
     * @param {string} sqlString - raw sql string
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<sqlResults>} If layer IS NOT a transaction and returnRecords is 'false', then it will return sql results object
     */
    async rawDatabase (sqlString = '') {

        this.debug('__rawDatabase');
        try {

            let statement = this.database.raw(sqlString);

            /**
             * if it is a transaction and also not a SELECT statement then return the SQL string statement
             */
            if (this.isTransaction && sqlString.trim().toUpperCase().indexOf('SELECT') !== 0) {

                let objSQL = statement.toSQL();
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                return await statement;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: '__rawDatabase' });

        }

    }

    /**
     * Access to the underlining search service
     * @param {string} obj - raw elasticsearch object
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async rawSearch (obj = {}) {

        this.debug('__rawSearch');
        try {

            return obj;

        } catch (error) {

            throw this.error.handle({ error, caller: '__rawSearch' });

        }

    }

    /**
     * Reindex a table in the search layer
     * @param {string} table - name of the table
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async reindexSearch (table = '') {

        this.debug('reindexSearch()');
        try {

            if (this.isTransaction === true) {

                throw new errors.Error({ message: 'reindexSearch can not be called inside a transaction' });

            }

            /**
             * 1. create new ES index
             * 2. start watching for updates using redis (for catching updates that happen while reindexing)
             * 3. using search with ES disabled start paging through and inserting into new ES index
             *    a. if timestamp exist, sort by it ASC.. if not use primaryKey
             * 4. once complete check redis watching and reinsert those.
             *    a. be sure to restart the watcher
             * 5. switch alias to new populated ES index
             * 6. check redis watcher once more and reinsert those.
             */

            // let result = await layers.reindexSearch('table', { options });

            return { table }; //delete this line

        } catch (error) {

            throw this.error.handle({ error, caller: 'reindexSearch' });

        }

    }

    /**
     * Rollbacks the current transaction
     * @returns {Promise<success>} return a success object
     * @throws {Error} rollback() can not be called inside a transaction
     */
    async rollback () {

        this.debug('rollback()');
        try {

            if (this.isTransaction === false) {

                throw new errors.Error({ message: 'rollback() can only be called inside a transaction' });

            }

            // await transaction.rollback(); // only needed if realTimeTransactions=true

            Object.keys(this.activeLocks).map((key) => {

                return this.activeLocks[key].unlock();

            });

            this.activeLocks = {};

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'rollback' });

        }

    }

    /**
     * Search a table from the data layer
     * @param {string} table - name of the table
     * @param {object=} options - available options
     * @param {string|array|object} [options.filter=''] - layerize filter syntax
     * @param {string|array} [options.joins=''] - raw sql join statement
     * @param {string|array} [options.sort=''] - fields to order by
     * @param {string|array} [options.group=''] - fields to group by
     * @param {string|array} [options.aggregates=''] - aggregates (requires group to be set)
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {number} [options.limit=50] - the amount of records to return
     * @param {number} [options.offset=0] - the offset of cursor
     * @param {boolean} [options.type=auto] - which datasource to use ['db','es','auto']
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - enforces any restrictions
     * @returns {Promise<results>} returns standard results object
     */
    async search (table = '', {
        filter = '',
        joins = '',
        sort = '',
        group = '',
        aggregates = '',
        fields = '*',
        limit = 50,
        offset = 0,
        includes = '',
        type = 'auto',
        readPermission = this.readPermission
    } = {}) {

        this.debug(`search(${table})`, { joins, group, aggregates });

        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(readPermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            let total = 0;
            let results = [];

            // clean arg data
            sort = (!sort) ? schema.primaryKey : sort;
            offset = (!offset) ? 0 : Math.floor(Math.abs(offset));
            limit = (!limit && limit !== 0) ? 50 : Math.floor(Math.abs(limit));

            if (typeof joins === 'string' && joins !== '') {

                joins = [ joins ];

            } else if (!Array.isArray(joins)) {

                joins = [];

            }

            if (joins.length > 0 && type === 'es') {

                throw new errors.Error({ message: 'joins can not be used when type=\'es\'.' });

            }

            if (schema.esEnabled && (type === 'auto' || type === 'es') && joins.length === 0) {

                this.debug('using elastic search for records');

                let es = {
                    index: `${this.schemaName}~${table}`,
                    type: 'index',
                    from: offset,
                    body: {}
                };

                if (limit > 0) {

                    es.size = limit;

                }

                ({ statement: es } = await this._applyFilters(es, { table, filter, authorization }));

                let esResponse = await this.es.search(es);

                total = esResponse.hits.total;

                let lngHits = esResponse.hits.hits.length;
                for (let i = 0;i < lngHits;i++) {

                    results.push(esResponse.hits.hits[i]._source);

                }

                ({ results } = await this._includes({ table, results, includes }));

                results = requestFields.parse(results, { stacked: [...authorization.properties, fields] });

            } else {

                this.debug('using db for records');

                // getting search sql statement and setting filter
                let statement = this.dbSchema.clone().table(table);
                ({ statement } = await this._applyFilters(statement, { table, filter, authorization }));

                // added joins if provided
                for (let i = 0; i < joins.length; i++) {

                    statement.joinRaw(joins[i]);

                }

                // getting count sql statement
                let countStatement = statement.clone().count(`${table}.${schema.primaryKey}`);

                if (joins.length > 0) {

                    if (!Array.isArray(fields)) {

                        fields = this.__toArray(fields, 4);

                    }

                    if (fields.length > 0) {

                        statement.columns(fields);

                    }

                }

                // set sort, offset, limit, group and aggregates.
                statement.orderBy(sort).offset(offset);

                if (limit > 0) {

                    statement.limit(limit);

                }

                results = await statement;
                total = parseInt((await countStatement)[0].count, 0);

                ({ results } = await this._includes({ table, results, includes }));

                if (joins.length === 0) {

                    results = requestFields.parse(results, { stacked: [...authorization.properties, fields] });

                }

            }

            return {
                sort,
                offset,
                limit,
                total,
                items: results
            };

        } catch (error) {

            throw this.error.handle({ error, caller: 'search' });

        }

    }

    /**
     * Returns a transaction to do calls against
     * @param {object=} options - available options
     * @param {boolean} [options.fields=this.realTimeTransactions] - use a traditional transaction
     * @returns {Layer} a new instance of a layer with transaction set to true
     * @throws {Error} transaction() can NOT be called inside a transaction
     */
    transaction ({ realTime = this.realTimeTransactions } = {}) {

        this.debug('transaction()');
        try {

            if (this.isTransaction === true) {

                throw new errors.Error({ message: 'transaction() can NOT be called inside a transaction' });

            }

            return new Layers({
                layerize: this.layerize,
                isTransaction: true,
                name: this.schemaName,
                database: this.database,
                schemas: this.schemas,
                cache: this.cache,
                search: this.es,
                validator: this.validator,
                realTimeTransactions: realTime,
                cacheExpireSeconds: this.cacheExpireSeconds
            });

        } catch (error) {

            throw this.error.handle({ error, caller: 'transaction' });

        }

    }

    /**
     * Updates a single full records into the data layer
     * @param {string} table - name of the table
     * @param {object} data - the full data of the record being updated
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecord=false] - will lock records for updating
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<object>} If layer IS NOT a transaction and returnRecord is 'true', then it will return the reocrd object
     * @returns {Promise<success>} If layer IS NOT a transaction and returnRecord is 'false', then it will return a success object
     */
    async update (table = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '*', includes = '', updatePermission = this.updatePermission } = {}) {

        this.debug(`update(${table})`);

        let layer = this;

        // a flag for knowing if we are using our own temporary transaction or not
        let internalTransaction = false;

        try {

            // if permission object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(updatePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            let key = data[schema.primaryKey];

            if (typeof key === 'undefined') {

                throw new errors.Error({ message: 'When using update(), the object must contain the primary key with all other properties.' });

            }

            // validate against json schema and remove any fields that are readonly, unless ignored
            await this._validate({ table, data, ignoreReadOnly });

            let auditLogs = [];

            // this could set to false if there is no differences in what is requested to save
            let saveUpdates = true;

            // if not a transaction, then create a temporary transaction layer to use for this update
            if (!this.isTransaction) {

                layer = this.transaction();
                internalTransaction = true;

            }

            let currentRecord = {};

            // check if currentRecord is needed, if so then make sure it is populated
            if (authorization.conditions.length > 0 || this.auditLogs) {

                // permission free because we want the full record, also permission free because we want the full record
                currentRecord = await layer.get(table, key, { forUpdate: true, readPermission: {} });

                ({ item: data, saveUpdates, auditLogs } = await this.__authorizingConditions({ authorization, currentRecords: [ currentRecord ], schema, key, item: data, table, auditLogs, saveUpdates }));

            }

            ({ data } = await this.__convertForDatabase({ table, data, ignoreReadOnly, schema }));

            if (saveUpdates) {

                let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(data);

                const cacheKey = this.cache.key(this.cacheKey, table, key);

                if (this.isTransaction) {

                    let objSQL;

                    // if elasticsearch is enabled we will return the updated record from the update query to update elastic search and cache data
                    if (schema.esEnabled) {

                        statement.returning('*');

                        objSQL = statement.toSQL();
                        objSQL.primaryKey = schema.primaryKey;
                        objSQL.populateToES = true;
                        objSQL.populateToCache = true;

                    } else { // if elasticsearch is disabled we will store cache key to clear, as the cache will repopulate the next time is it requested

                        objSQL = statement.toSQL();
                        objSQL.cache = [ cacheKey ];

                    }

                    objSQL.keys = [ key ];
                    objSQL.auditLogs = auditLogs;
                    objSQL.table = table;
                    this.transactions.push(objSQL);
                    return objSQL;

                } else {

                    let results = await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys: [ key ], schema, statement, returnRecords: returnRecord, fields, includes, authorization, auditLogs, layer });

                    if (returnRecord) {

                        return results[0];

                    } else {

                        return results;

                    }

                }

            } else { // no save was needed

                if (this.isTransaction) {

                    return null;

                } else {

                    // cancel transaction and unlock records as no changes need
                    await layer.rollback();

                    if (returnRecord) {

                        return currentRecord;

                    }

                    return { success: true };

                }

            }

        } catch (error) {

            if (internalTransaction && layer.isTransaction) {

                await layer.rollback();

            }

            throw this.error.handle({ error, caller: 'update()' });

        }

    }

    /**
     * Update many full records by key into the data layer
     * @param {string} table - name of the table
     * @param {array} data - the full data of the records being updated
     * @param {object=} options - available options
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {permission} [options.permission={}] - permission object
     * @returns {Promise<objSQL>} If layer IS a transaction, then it will return objSQL
     * @returns {Promise<array>} If layer IS NOT a transaction and returnRecord is 'true', then it will return the array of record objects
     * @returns {Promise<success>} If layer IS NOT a transaction and returnRecord is 'false', then it will return a success object
     */
    async updateMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '*', includes = '', updatePermission = this.updatePermission } = {}) {

        this.debug(`updateMany(${table})`);

        let layer = this;

        // a flag for knowing if we are using our own temporary transaction or not
        let internalTransaction = false;

        try {

            // if permssion object is passed it verifies and throws error if not authorized and returns authorization if valid
            let authorization = await this.permissions.authorized(updatePermission);

            const schema = this.schemas.layerize[table];

            if (typeof schema === 'undefined') {

                throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

            }

            /**
             * 1. loop through each record
             *      a. make sure the primary key exists, if not throw error
             *      b. validate data against original Schema
             *      c. return error as soon as one is found
             *      d. update timestamps
             *      e. build seperate update sql statement for each
             *      f. get cache key
             * 2. create transaction if not part of one
             */

            let keys = [];

            // looping seperately so we can through an error if primary key is not present
            for (let i = 0; i < data.length; i++) {

                let item = data[i];

                let key = item[schema.primaryKey];

                // storing key for later populating ES
                keys.push(key);

                // a. make sure the primary key exists, if not throw error
                if (typeof item[schema.primaryKey] === 'undefined') {

                    throw new errors.Error({ message: 'When using updateMany(), each object in the data array must contain the primary key with all other properties. If you are trying to pass a partial objects use patchMany().' });

                }

                // b. validate data against original, return error as soon as one is found
                await this._validate({ table, data: item, ignoreReadOnly });

            }

            // if not a transaction, then create a temporary transaction layer to use for this update
            if (!this.isTransaction) {

                layer = this.transaction();
                internalTransaction = true;

            }

            let currentRecords = [];

            // check if currentRecord is needed, if so then make sure it is populated
            if (authorization.conditions.length > 0 || this.auditLogs) {

                // merge pulled in current records with previous passed current records
                currentRecords = await layer.getMany(table, keys, { forUpdate: true, readPermission: {} });

            }

            let auditLogs = [];
            let statements = [];
            for (let j = 0; j < data.length; j++) {

                let item = data[j];

                let key = item[schema.primaryKey];

                let saveUpdates = true;

                // check if currentRecord is needed, if so then make sure it is populated
                if (authorization.conditions.length > 0 || this.auditLogs) {

                    ({ item, saveUpdates, auditLogs } = await this.__authorizingConditions({ authorization, currentRecords, schema, key, item, table, auditLogs, saveUpdates }));

                }

                ({ data: item } = await this.__convertForDatabase({ table, data: item, ignoreReadOnly, schema }));

                item[schema.primaryKey] = key;

                data[j] = item;

                if (saveUpdates) {

                    // e. build seperate update sql statement for each
                    let objSQL = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(item).toSQL();

                    // f. get cache key
                    let cacheKey = await this.cache.key(this.cacheKey, table, key);

                    objSQL.cache = [ cacheKey ];
                    statements.push(objSQL);

                }

            }

            if (this.isTransaction) {

                statements[0].table = table;
                statements[0].keys = keys;

                if (schema.esEnabled) {

                    // attach to first record as it does not need to be attached to others because it contains enough information to repopulate for all
                    statements[0].populateToES = true;

                }

                if (auditLogs.length > 0) {

                    // attach to first record as it does not need to be attached to others or it will cause a duplicate logs error
                    statements[0].auditLogs = auditLogs;

                }

                this.transactions = this.transactions.concat(statements);

                return statements;

            } else {

                return await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys, schema, statement: statements, returnRecords, fields, includes, authorization, auditLogs, layer });

            }

        } catch (error) {

            if (internalTransaction && layer.isTransaction) {

                await layer.rollback();

            }

            throw this.error.handle({ error, caller: 'updateMany' });

        }

    }

    // let raw = layers.raw();
    // raw.search; // elasticsearch module
    // raw.cache; // ioredis module
    // raw.database; // knex module

    /**
     * A protected internal method for attaching a list from another table to current object.
     * @access protected
     * @param {array} data - the full data of the records being updated
     * @param {object=} options - available options
     * @param {string} [options.table=''] - name of the table
     * @param {string} [options.includeId=''] - name of the table
     * @param {array} [options.results=[]] - an array of data that is requesting to be extended
     * @returns {Promise<array>} The extended passed in results
     * @throws {Error} Unable to find include relationship between '{table}' and '{includeId}'.
     */
    async _attachListToObject ({ table = '', includeId = '', results = [] } = {}) {

        this.debug(`_attachListToObject(${table})`);
        try {

            if (results.length > 0) {

                const objDataMap = {};

                if (typeof this.schemas.includes[table].referenced[includeId] === 'undefined') {

                    throw new errors.Error({ message: `Unable to find include relationship between '${table}' and '${includeId}'.` });

                }

                let reference = this.schemas.includes[table].referenced[includeId];
                let propertyTable = reference.propertyTable;
                let property = reference.property;
                let attachKey = reference.column;
                let filter = [];

                for (let i = 0; i < results.length; i++) {

                    let result = results[i];

                    if (result[attachKey] !== '') {

                        // create a map for easy reference later
                        if (typeof objDataMap[result[attachKey]] === 'undefined') {

                            filter.push(`${property}:${result[attachKey]}`);
                            objDataMap[result[attachKey]] = [];

                        }

                        // if key exists more than once just save where to plug it in
                        objDataMap[result[attachKey]].push(i);

                    }

                    //create place holder since there no way to link
                    result[includeId] = [];

                }

                let list = await this.search(propertyTable, { filter, limit: 0 });

                for (let x = 0; x < list.items.length; x++) {

                    let item = list.items[x];

                    let locations = objDataMap[item[property]];
                    for (let j = 0; j < locations.length; j++) {

                        let location = locations[j];
                        results[location][includeId].push(item);

                    }

                }

            }

            return { results };

        } catch (error) {

            throw this.error.handle({ error, caller: '_attachListToObject' });

        }

    }

    /**
     * A protected internal method for attaching a record from another table to current object.
     * @access protected
     * @param {array} data - the full data of the records being updated
     * @param {object=} options - available options
     * @param {string} [options.table=''] - name of the table
     * @param {string} [options.includeId=''] - name of the table
     * @param {array} [options.results=[]] - an array of data that is requesting to be extended
     * @returns {Promise<array>} The extended passed in results
     * @throws {Error} Unable to find include relationship between '{table}' and '{includeId}'.
     */
    async _attachToObject ({ table = '', includeId = '', results = [] } = {}) {

        this.debug(`_attachToObject(${table})`);
        try {

            if (results.length > 0) {

                const schema = this.schemas.layerize[table];

                if (typeof schema === 'undefined') {

                    throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

                }

                const objDataMap = {};

                if (typeof schema.includes[includeId] === 'undefined') {

                    throw new errors.Error({ message: `Unable to find include relationship between '${table}' and '${includeId}'.` });

                }

                let property = schema.includes[includeId].property;
                let attachTable = schema.includes[includeId].table;
                let attachKey = schema.includes[includeId].column;

                for (let i = 0; i < results.length; i++) {

                    let result = results[i];

                    if (result[property] !== '') {

                        // create a map for easy reference later
                        if (typeof objDataMap[result[property]] === 'undefined') {

                            objDataMap[result[property]] = [];

                        }

                        // if key exists more than once just save where to plug it in
                        objDataMap[result[property]].push(i);

                    }

                    //create place holder since there no way to link
                    result[includeId] = {};

                }

                let keys = Object.keys(objDataMap);

                // setting readPermission to blank until we are able to set it for attachTable
                let items = await this.getMany(attachTable, keys, { readPermission: {} });

                for (let x = 0; x < items.length; x++) {

                    let item = items[x];

                    let locations = objDataMap[item[attachKey]];
                    for (let j = 0; j < locations.length; j++) {

                        let location = locations[j];
                        results[location][includeId] = item;

                    }

                }

            }

            return { results };

        } catch (error) {

            throw this.error.handle({ error, caller: '_attachToObject' });

        }

    }

    /**
     * A protected internal method for applying the search criteria 'filter' to a db or es statement.
     * @access protected
     * @param {knex|object} statement - the db or es statement
     * @param {object=} options - available options
     * @param {string} [options.table=''] - name of the table
     * @param {string|array|object} [options.filter=''] - layerize filter syntax
     * @param {object} [options.authorization={}] - authorization
     * @returns {Promise<knex|object>} The extended passed in statement
     * @throws {Error} A native DB filter can not be used within an ES query.
     * @throws {Error} A native ES filter can not be used within a DB query.
     */
    async _applyFilters (statement, { table = '', filter = '', authorization = {} } = {}) {

        this.debug(`_applyFilters(${table})`);
        try {

            let isEsStatement = false;
            if (typeof statement.index !== 'undefined' && typeof statement.type !== 'undefined' && typeof statement.body !== 'undefined') {

                isEsStatement = true;

            }

            let objFilter = this._stringToObjectFilters({ table, filter });

            if (objFilter.native.active) {

                if (isEsStatement) {

                    if (objFilter.native.type === 'db') {

                        throw new errors.Error({ message: 'A native DB filter can not be used within an ES query.' });

                    }

                    statement.body = objFilter.native.body;

                    if (Object.keys(statement.body).length === 0) {

                        statement.body = {
                            query: {
                                match_all: {}
                            }
                        };

                    }

                } else {

                    if (objFilter.native.type === 'es') {

                        throw new errors.Error({ message: 'A native ES filter can not be used within a DB query.' });

                    }

                    let where = await this.permissions.authorizedFilter('db', authorization);

                    if (where !== '' && objFilter.native.where !== '') {

                        where = `(${where}) AND (${objFilter.native.where})`;

                    } else if (objFilter.native.where !== '') {

                        where = objFilter.native.where;

                    }

                    statement.whereRaw(where);

                }

            } else {

                if (isEsStatement) {

                    if (objFilter.structure.length > 0) {

                        statement.body = this._esFilter(objFilter, authorization);

                    }

                    if (Object.keys(statement.body).length === 0) {

                        statement.body = {
                            query: {
                                match_all: {}
                            }
                        };

                    }

                } else {

                    let strFilter = await this._dbFilter(objFilter, authorization);
                    if (strFilter !== '') {

                        statement.whereRaw(strFilter);

                    }

                }

            }

            return { statement };

        } catch (error) {

            throw this.error.handle({ error, caller: '_applyFilters' });

        }

    }

    /**
     * A protected internal method for starting a tradition sql transaction.
     * @access protected
     * @returns {transaction} A KnexJS transaction
     */
    _createTransaction () {

        this.debug('_createTransaction()');
        try {

            return new Promise((resolve) => this.database.transaction(resolve));

        } catch (error) {

            throw this.error.handle({ error, caller: '_createTransaction' });

        }

    }

    /**
     * A protected internal method for generating the db where statement.
     * @access protected
     * @param {layerizeFilter} objFilter - layerize object filter
     * @param {object} [authorization={}] - authorization
     * @returns {Promise<string>} db sql where statement
     */
    async _dbFilter (objFilter = {}, authorization = {}) {

        this.debug('_dbFilter');
        try {

            let where = await this.permissions.authorizedFilter('db', authorization);

            let lngFilters = objFilter.structure.length;
            if (lngFilters === 0) {

                return where;

            }

            if (lngFilters > 0) {

                if (where !== '') {

                    where = `(${where}) AND `;

                }

                let cntArg = 0;
                for (let i = 0; i < lngFilters; i++) {

                    let filter = objFilter.structure[i];

                    if (filter === 'arg') {

                        let objArgName;
                        let objArg = objFilter.columns[cntArg];
                        let strArg = '';
                        let objArgValue = this.__clean(objArg.value);
                        let bolIsJsonbSearch = false;

                        switch (objArg.type) {

                            case 'jsonb':
                                switch (objArg.sep) {

                                    case '??':
                                        objArgName = objArg.name;
                                        break;
                                    default:
                                        let strSub = objArgValue.substring(0, 1);
                                        let valueOperator = '#>>';
                                        if (strSub === '{' || strSub === '[') {

                                            try {

                                                JSON.parse(objArgValue);
                                                valueOperator = '#>';
                                                bolIsJsonbSearch = true;

                                                if (objArg.sep === '!=') {

                                                    objArg.sep = '!@';

                                                } else {

                                                    objArg.sep = '@>';

                                                }

                                            } catch (e) {

                                                valueOperator = '#>>';
                                                bolIsJsonbSearch = false;

                                            }

                                        }

                                        if (typeof objArg.keyName !== 'undefined') {

                                            objArgName = `${objArg.name} ${valueOperator} '{${objArg.keyName}}'`;

                                        } else {

                                            objArgName = `${objArg.name} ${valueOperator} '{}'`;

                                        }

                                }

                                break;
                            default:
                                objArgName = objArg.name;

                        }
                        if (objArg.caseInsensitive && !bolIsJsonbSearch) {

                            objArgName = `lower(${objArgName})`;
                            objArgValue = objArgValue.toLowerCase();

                        }
                        if (objArg.blankToNull && (objArg.value === '' || (objArg.value).toLowerCase() === 'null') && !bolIsJsonbSearch) {

                            switch (objArg.sep) {

                                case '==':
                                    objArg.sep = '^^';
                                    break;
                                case '!=':
                                    objArg.sep = '!^';
                                    break;

                            }

                        }
                        switch (objArg.sep) {

                            case '??':
                                strArg = `${objArgName} ? '${objArgValue}'`;
                                break;
                            case '=#':
                                strArg = `${objArgName} ='${objArgValue}'`;
                                break;
                            case '>>':
                                strArg = `${objArgName} > '${objArgValue}'`;
                                break;
                            case '<<':
                                strArg = `${objArgName} < '${objArgValue}'` ;
                                break;
                            case '>=':
                                strArg = `${objArgName} >= '${objArgValue}'`;
                                break;
                            case '<=':
                                strArg = `${objArgName} <= '${objArgValue}'`;
                                break;
                            case '==':
                                strArg = `${objArgName} = '${objArgValue}'`;
                                break;
                            case '@>':
                                strArg = `${objArgName} @> '${objArgValue}'`;
                                break;
                            case '!@':
                                strArg = `NOT ${objArgName} @> '${objArgValue}'`;
                                break;
                            case '!=':
                                strArg = `${objArgName} != '${objArgValue}'`;
                                break;
                            case '!^':
                                strArg = `${objArgName} IS NOT NULL`;
                                break;
                            case '^^':
                                strArg = `${objArgName} IS NULL`;
                                break;
                            case '%%':
                                strArg = `${objArgName} LIKE '%${objArgValue}%'`;
                                break;
                            case '%a':
                                strArg = `${objArgName} LIKE '%%${objArgValue}'`;
                                break;
                            case 'a%':
                                strArg = `${objArgName} LIKE '${objArgValue}%%'`;
                                break;

                        }
                        where += strArg;
                        cntArg++;

                    } else {

                        let strOp = '';
                        switch (filter) {

                            case '&&':
                                strOp = ' AND ';
                                break;
                            case '||':
                                strOp = ' OR ';
                                break;
                            default:
                                strOp = filter;
                                break;

                        }
                        where += strOp;

                    }

                }

            }
            return where;

        } catch (error) {

            throw this.error.handle({ error, caller: '_dbFilter' });

        }

    }

    /**
     * A protected internal method for generating the elasticsearch body object.
     * @access protected
     * @param {layerizeFilter} objFilter - layerize object filter
     * @param {object} [authorization={}] - authorization
     * @returns {object} elasticsearch body object
     */
    _esFilter (objFilter = {}, authorization = {}) {

        this.debug('_esFilter');
        try {

            let cntArg = 0;

            /**
             * private internal function for _esFilter
             * @access private
             * @param {array} ary - layerize object filter
             * @returns {object} es body statement
             */
            const parseIntoArrays = (ary = []) => {

                let structure = [];
                let lngAry = ary.length;
                let x = 0;
                for (x = 0;x < lngAry;x++) {

                    let item = ary[x];
                    if (item === ')') {

                        break;

                    } else if (item === '(') {

                        let obj = parseIntoArrays(ary.slice(x + 1));
                        x = x + (obj.x + 1);
                        structure.push(obj.structure);

                    } else {

                        structure.push(item);

                    }

                }

                return { structure, x };

            };

            let objStructure = parseIntoArrays(objFilter.structure);
            let structure = objStructure.structure;

            /**
             * private internal function for _esFilter
             * @access private
             * @param {array} ary - layerize object filter
             * @returns {object} es body statement
             */
            const boolFilter = (ary = []) => {

                let obj = {
                    bool: {}
                };

                let aryFilter = [];
                let lngAry = ary.length;
                for (let i = 0;i < lngAry;i++) {

                    let filter = ary[i];
                    if (Array.isArray(filter)) {

                        aryFilter.push(boolFilter(filter));

                    } else {

                        if (filter === 'arg') {

                            let objArg = objFilter.columns[cntArg];
                            let aryArgName = objArg.name.split('.');
                            let objArgName = aryArgName[aryArgName.length - 1];
                            let objArgValue = objArg.value;
                            let objArgument = {};
                            let aryArgValue = [];

                            if (typeof objArg.keyName !== 'undefined') {

                                objArgName += `.${objArg.keyName}`;

                            }

                            objArgName = objArgName.replace(/\,/gi, '.');
                            aryArgValue = this.__toArray(objArgValue, 3);

                            switch (objArg.sep) {

                                case '??':
                                    // strArg = objArgName + ' ? \'' + objArgValue + '\' ';
                                    break;
                                case '=#':
                                    // strArg = objArgName + '=' + objArgValue + ' ';
                                    break;
                                case '>>':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { range: {} };
                                            objStatement.range[objArgName] = {
                                                gt: argValue
                                            };
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { range: {} };
                                        objArgument.range[objArgName] = {
                                            gt: objArgValue
                                        };

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '<<':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { range: {} };
                                            objStatement.range[objArgName] = {
                                                lt: argValue
                                            };
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { range: {} };
                                        objArgument.range[objArgName] = {
                                            lt: objArgValue
                                        };

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '>=':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { range: {} };
                                            objStatement.range[objArgName] = {
                                                gte: argValue
                                            };
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { range: {} };
                                        objArgument.range[objArgName] = {
                                            gte: objArgValue
                                        };

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '<=':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { range: {} };
                                            objStatement.range[objArgName] = {
                                                lte: argValue
                                            };
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { range: {} };
                                        objArgument.range[objArgName] = {
                                            lte: objArgValue
                                        };

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '==':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { term: {} };
                                            objStatement.term[objArgName] = argValue;
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { term: {} };
                                        objArgument.term[objArgName] = objArgValue;

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '@>':
                                    // strArg = objArgName + '@>\'' + objArgValue + '\' ';
                                    break;
                                case '!=':
                                    objArgument = { bool: { must_not: [] } };
                                    if (aryArgValue.length > 1) {

                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { term: {} };
                                            objStatement.term[objArgName] = argValue;
                                            objArgument.bool.must_not.push(objStatement);

                                        });

                                    } else {

                                        let objStatement = { term: {} };
                                        objStatement.term[objArgName] = objArgValue;
                                        objArgument.bool.must_not.push(objStatement);

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '!^':
                                    // strArg = objArgName + ' IS NOT NULL ';
                                    break;
                                case '^^':
                                    // strArg = objArgName + ' IS NULL ';
                                    break;
                                case '%%':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { wildcard: {} };
                                            objStatement.wildcard[objArgName] = `*${argValue}*`;
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { wildcard: {} };
                                        objArgument.wildcard[objArgName] = `*${objArgValue}*`;

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case '%a':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { wildcard: {} };
                                            objStatement.wildcard[objArgName] = `*${argValue}`;
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { wildcard: {} };
                                        objArgument.wildcard[objArgName] = `*${objArgValue}`;

                                    }
                                    aryFilter.push(objArgument);
                                    break;
                                case 'a%':
                                    if (aryArgValue.length > 1) {

                                        objArgument = { bool: { must: [] } };
                                        aryArgValue.forEach((argValue) => {

                                            let objStatement = { wildcard: {} };
                                            objStatement.wildcard[objArgName] = `${argValue}*`;
                                            objArgument.bool.must.push(objStatement);

                                        });

                                    } else {

                                        objArgument = { wildcard: {} };
                                        objArgument.wildcard[objArgName] = `${objArgValue}*`;

                                    }
                                    aryFilter.push(objArgument);
                                    break;

                            }
                            cntArg++;

                        }

                    }

                }

                if (lngAry > 1) {

                    switch (ary[1]) {

                        case '&&':
                            obj.bool.must = aryFilter;
                            break;
                        case '||':
                            obj.bool.should = aryFilter;
                            break;

                    }

                } else {

                    obj.bool.must = aryFilter;

                }

                return obj;

            };

            return {
                query: {
                    constant_score: {
                        filter: boolFilter(structure)
                    }
                }
            };

        } catch (error) {

            throw this.error.handle({ error, caller: '_esFilter' });

        }

    }

    /**
     * A protected internal method for attaching other list of records and individual records into the current provide records.
     * @access protected
     * @param {object=} options - available options
     * @param {string} options.table - name of the table
     * @param {array} [options.results=[]] - an array of data that is requesting to be extended
     * @param {string|array} [options.includes=''] - an array or comma seperate string of includes
     * @returns {Promise<array>} the array of results
     * @throws {Error} Unable to find include relationship between '{table}' and '{includeId}'.
     */
    async _includes ({ table = '', results = [], includes = '' } = {}) {

        this.debug(`_includes(${table})`, { includes });
        try {

            if (!Array.isArray(includes)) {

                if (includes === '') {

                    includes = [];

                } else {

                    includes = this.__toArray(includes, 4);

                }

            }

            for (let i = 0; i < includes.length; i++) {

                let includeId = includes[i];

                this.debug('_includes id:', includeId);

                if (typeof this.schemas.includes[table].source[includeId] === 'undefined' && typeof this.schemas.includes[table].referenced[includeId] === 'undefined') {

                    throw new errors.Error({ message: `Unable to find include relationship between '${table}' and '${includeId}'.` });

                }

                if (typeof this.schemas.includes[table].source[includeId] !== 'undefined') {

                    ({ results } = await this._attachToObject({ table, includeId, results }));

                }

                if (typeof this.schemas.includes[table].referenced[includeId] !== 'undefined') {

                    ({ results } = await this._attachListToObject({ table, includeId, results }));

                }

            }

            return { results };

        } catch (error) {

            throw this.error.handle({ error, caller: '_includes' });

        }

    }

    /**
     * A protected internal method for merging a patch to a currect record object. Any arrays will be replaced in full at highest level if patch contains the array property.
     * @access protected
     * @param {object=} options - available options
     * @param {object} [options.currentRecord={}] - current record
     * @param {object} [options.patch={}] - patch to merge in to current record
     * @returns {object} patched record object
     */
    _mergePatch ({ currentRecord = {}, patch = {}, schema = {} } = {}) {

        this.debug('_mergePatch');
        try {

            let data = extend(true, {}, currentRecord);

            /**
             * A private internal method for crawling _mergePatch.
             * @access private
             * @param {object} [newObj={}] - current record
             * @param {object} [patch={}] - patch to merge in to current record
             * @param {object} [properties={}] - object schema properties
             * @returns {object} patched object
             */
            let crawl = (newObj = {}, patch = {}, properties = {}) => {

                let keys = Object.keys(patch);
                for (let i = 0; i < keys.length; i++) {

                    let key = keys[i];
                    // it only crawls objects if a schema is defined for it, if no schema it does a full replace on that object
                    if (typeof patch[key] === 'object' && !Array.isArray(patch[key]) && typeof properties[key] !== 'undefined' && patch[key] !== null && Object.keys(patch[key]).length > 0) {

                        newObj[key] = crawl(newObj[key], patch[key], properties[key].properties);

                    } else {

                        newObj[key] = patch[key];

                    }

                }

                return newObj;

            };

            return { data: crawl(data, patch, schema.properties) };

        } catch (error) {

            throw this.error.handle({ error, caller: '_mergePatch' });

        }

    }

    /**
     * A protected internal method for narrow the schema to the requested properties so it can be validated.
     * @access protected
     * @param {object=} options - available options
     * @param {schema} options.schema - schema definition
     * @param {array} [options.properties=[]] - properties that should be included
     * @returns {schema} a newly generated schema
     */
    _narrowedSchema ({ schema = {}, properties = [] } = {}) {

        this.debug('_narrowedSchema');
        try {

            let narrowedSchema = {
                type: 'object',
                $async: true,
                properties: {},
                dynamicDefaults: {},
                required: []
            };

            for (let i = 0; i < properties.length; i++) {

                let property = properties[i];
                narrowedSchema.properties[property] = schema.properties[property];

                if (typeof schema.dynamicDefaults[property] !== 'undefined') {

                    narrowedSchema.dynamicDefaults[property] = schema.dynamicDefaults[property];

                }

                if (schema.required.indexOf(property) > 0) {

                    narrowedSchema.required.push(property);

                }

            }

            return narrowedSchema;

        } catch (error) {

            throw this.error.handle({ error, caller: '_narrowedSchema' });

        }

    }

    /**
     * A protected internal method for running watchers.
     * @access protected
     * @param {object=} options - available options
     * @param {array} [options.tables=[]] - an array of tables that have changed
     * @returns {Promise<array>} the array of results
     */
    async _runWatcher ({ tables = [] }) {

        this.debug('_runWatcher');
        try {

            for (let i = 0; i < tables.length; i++) {

                let obj = tables[i];
                if (typeof this.layerize.watchers[obj.table] !== 'undefined') {

                    if (typeof this.layerize.watchers[obj.table][obj.on] !== 'undefined') {

                        let actions = Object.keys(this.layerize.watchers[obj.table][obj.on]);

                        for (let x = 0; x < actions.length; x++) {

                            let action = actions[x];
                            await this.layerize.watchers[obj.table][obj.on][action](obj.keys, { layerize: this.layerize, schemaName: this.schemaName });

                        }

                    }

                }

            }

        } catch (error) {

            throw this.error.handle({ error, caller: '_runWatcher' });

        }

    }

    /**
     * A protected internal method for saving audit logs.
     * @access protected
     * @param {object=} options - available options
     * @param {array} [options.logs=[]] - an array of logs to be saved
     * @returns {Promise<array>} the array of results
     */
    async _saveAuditLogs ({ logs = [] }) {

        this.debug('_saveAuditLogs');
        try {

            // save audit logs to DB
            await this.dynamoTables.table('audit-logs', this.schemaName).create(logs);

        } catch (error) {

            throw this.error.handle({ error, caller: '_saveAuditLogs' });

        }

    }

    /**
     * A protected internal method for attaching other list of records and individual records into the current provide records.
     * @access protected
     * @param {object=} options - available options
     * @param {string} options.table - name of the table
     * @param {string|array|object} [options.filter=''] - layerize filter syntax
     * @returns {layerizeFilter} returns the layerizeFilter
     */
    _stringToObjectFilters ({ table = '', filter = '' } = {}) {

        this.debug(`_stringToObjectFilters(${table})`);

        const schema = this.schemas.layerize[table];

        if (typeof schema === 'undefined') {

            throw new errors.Error({ message: `The schema '${table}' does not exist in layerize.` });

        }

        let objFilter = {
            raw: [],
            columns: [],
            structure: [],
            query: {},
            native: {
                active: false,
                type: '',
                where: '',
                body: {}
            }
        };

        if (typeof filter === 'object' && !Array.isArray(filter)) {

            if (typeof filter.native === 'boolean' && filter.native === true && (typeof filter.where === 'string' || typeof filter.query === 'object')) {

                objFilter.native.active = true;

                if (typeof filter.where === 'string') {

                    objFilter.native.type = 'db';
                    objFilter.native.where = filter.where;

                } else if (typeof filter.query === 'object') {

                    objFilter.native.type = 'es';

                    delete filter.native;
                    objFilter.native.body = extend(true, {}, filter);

                }

            }

        }

        if (!objFilter.native.active) {

            if (Array.isArray(filter)) {

                let strFilter = '';
                filter = filter.filter(obj => obj !== '');
                for (let i = 0; i < filter.length; i++) {

                    if (strFilter !== '') {

                        strFilter += '&&';

                    }

                    if (filter.length === 1) {

                        strFilter += filter[i];

                    } else {

                        strFilter += `(${filter[i]})`;

                    }

                }

                filter = strFilter;

            }

            if (filter === '') {

                return objFilter;

            }

            ({ objFilter } = this.__buildFilter({ schema, objFilter, filter }));

            this._validateFilter(objFilter);

        }
        return objFilter;

    }

    /**
     * A protected internal method for attaching other list of records and individual records into the current provide records.
     * @access protected
     * @param {object=} options - available options
     * @param {string} options.table - name of the table
     * @param {object} [options.data={}] - data to be validated
     * @param {schema=} [options.schema] - JSON schema defintion, if not passed it uses table default schema
     * @returns {Promise<boolean>} returns a true boolean
     */
    async _validate ({ table = '', data = {}, schema = {} } = {}) {

        this.debug(`_validate(${table})`);
        try {

            try {

                if (Object.keys(schema).length) {

                    // use passed schema
                    await this.validator.validate(schema, data);

                } else {

                    // use preloaded schema
                    await this.validator.validate(table, data);

                }

            } catch (e) {

                throw e;

            }

            return true;

        } catch (error) {

            throw this.error.handle({ error, caller: '_validate' });

        }

    }

    /**
     * A protected internal method for validating the filter.
     * @access protected
     * @param {layerizeFilter} objFilter - layerize object filter
     * @returns {success} returns success
     */
    _validateFilter (objFilter) {

        let aryOps = objFilter.structure;
        let prevOps = '';
        let nextOps = '';
        let lngOps = aryOps.length;
        let strTest = '';
        for (let i = 0;i < lngOps;i++) {

            let operator = aryOps[i];
            switch (operator) {

                case '&&':
                case '||':
                    //can not be end
                    if (lngOps === i + 1) {

                        throw new errors.Error({ message: 'filters: ' + operator + ' must be followed by an argument.' });

                    }

                    //can not be start
                    if (i === 0) {

                        throw new errors.Error({ message: 'filters: ' + operator + ' can not lead an argument.' });

                    }

                    //must follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (!(prevOps === 'arg' || prevOps === ')')) {

                            throw new errors.Error({ message: 'filters: ' + operator + ' can not lead an argument.' });

                        }

                    }

                    //must be followed by arg,(
                    if (lngOps > i + 1) {

                        nextOps = aryOps[i + 1];
                        if (!(nextOps === 'arg' || nextOps === '(')) {

                            throw new errors.Error({ message: 'filters: ' + operator + ' must be followed by an argument.' });

                        }

                    }
                    strTest += '+';
                    break;
                case '(':
                    //can not be end
                    if (lngOps === i + 1) {

                        throw new errors.Error({ message: 'filters: ' + operator + ' must be followed by an argument.' });

                    }

                    //can not follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (prevOps === 'arg' || prevOps === ')') {

                            throw new errors.Error({ message: 'filters: ' + operator + ' can only follow an operator.' });

                        }

                    }
                    strTest += '(';
                    break;
                case ')':
                    //can not be start
                    if (i === 0) {

                        throw new errors.Error({ message: 'filters: ' + operator + ' can not lead an argument.' });

                    }

                    //must follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (!(prevOps === 'arg' || prevOps === ')')) {

                            throw new errors.Error({ message: 'filters: ' + operator + ' must follow an argument.' });

                        }

                    }
                    strTest += ')';
                    break;
                default:
                    //if not end then must be followed by &&,||,)
                    if (lngOps > i + 1) {

                        nextOps = aryOps[i + 1];
                        if (nextOps === 'arg' || nextOps === '(') {

                            throw new errors.Error({ message: 'filters: arguments can not be followed by arguments.' });

                        }

                    }
                    strTest += '0';
                    break;

            }

        }

        //validate brackets ()... strTest string built in the for loop above;
        try {

            /* eslint-disable no-eval */
            eval(strTest); //controlled eval test.. IT IS SAFE!
            /* eslint-enable no-eval */

        } catch (e) {

            throw new errors.Error({ message: 'filters: filter arguments "()" not structured properly.' });

        }

        return { success: true };

    }

    /**
     * A protected internal method for authorizing based on permission conditions.
     * @access protected
     * @param {object=} options - available options
     * @param {string} options.table - name of the table
     * @param {string} [options.type=update] - type of action, can be 'insert' or 'update'
     * @param {object} [options.data={}] - data to be validated
     * @param {schema=} [options.schema] - JSON schema defintion, if not passed it uses table default schema
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @returns {Promise<object>} returns an object containing the passed in data parameter
     */
    async __authorizingConditions ({ authorization, currentRecords, schema, key = '', item = {}, table = '', auditLogs = [], saveUpdates = true }) {

        try {

            // check if currentRecord is needed, if so then make sure it is populated
            if (authorization.conditions.length > 0 || this.auditLogs) {

                let idx = currentRecords.findIndex(r => r[schema.primaryKey] === key);
                let currentRecord = currentRecords[idx];

                // if authorization has conditions then do a check if record is allowed to be updated
                if (authorization.conditions.length > 0) {

                    // returns record object if valid and null if invalid
                    let allowedRecord = await this.permissions.authorizedData(authorization, currentRecord);

                    if (allowedRecord === null) {

                        throw new errors.Error({ statusCode: 403, message: `You do not have permission to update record '${key}' in '${table}' table.` });

                    }

                }

                if (authorization.properties.length > 0) {

                    // returns a clean data object with unauthorized fields removed or replaced with currentRecord data
                    item = await this.permissions.authorizedProperties(authorization, item, currentRecord);

                }

                // if audit logs are enabled then we need to figure out what has changed on the record
                if (this.auditLogs) {

                    let copyCurrentRecord = extend(true, {}, currentRecord);

                    // loop through copyCurrentRecord TOP LEVEL properties and remove properties from the copyCurrentRecord that do not exist in data
                    let currentRecordProperties = Object.keys(copyCurrentRecord);
                    for (let x = 0; x < currentRecordProperties.length; x++) {

                        let propertyName = currentRecordProperties[x];
                        if (typeof item[propertyName] === 'undefined') {

                            delete copyCurrentRecord[propertyName];

                        }

                        if (typeof item[propertyName] !== typeof copyCurrentRecord[propertyName]) {

                            throw new errors.Error({ statusCode: 500, message: `auditLogs: When comparing the passed record with the original record, the '${propertyName}' property is a different typeof '${typeof item[propertyName]}' vs '${typeof copyCurrentRecord[propertyName]}'. Passed value: '${item[propertyName]}' vs Original value: '${copyCurrentRecord[propertyName]}'` });

                        }

                    }

                    let differences = objects.difference(copyCurrentRecord, item) || [];

                    if (differences.length > 0) {

                        auditLogs.push({
                            id: uuid(),
                            table,
                            record_id: key,
                            type: 'update',
                            differences: JSON.stringify(differences)
                        });

                    } else {

                        // no changes found so no reason to save
                        saveUpdates = false;

                    }

                }

            }

            return { item, saveUpdates, auditLogs };

        } catch (error) {

            throw this.error.handle({ error, caller: '__authorizingConditions' });

        }

    }

    /**
     * A private internal method for validating the filter.
     * @access private
     * @param {schema} options.schema - JSON schema defintion
     * @param {layerizeFilter} options.objFilter - layerize object filter
     * @param {string} [options.filter=''] - layerize filter syntax
     * @returns {object} returns object contains objFilter property
     */
    __buildFilter ({ schema = {}, objFilter = {}, filter = '' } = {}) {

        this.debug('__buildFilter');
        try {

            objFilter.raw = this.__toArray(filter, 2);
            let lngFilters = objFilter.raw.length;
            for (let i = 0;i < lngFilters;i++) {

                let filtr = objFilter.raw[i];
                switch (filtr) {

                    case '&&':
                    case '||':
                    case '(':
                    case ')':
                        objFilter.structure.push(filtr);
                        break;
                    default:
                        let aryArgs = this.__toArray(filtr, 1);

                        let schemaName = schema.name;
                        let columnName = aryArgs[0];
                        let aryColumnName = aryArgs[0].split('.');

                        if (aryColumnName.length > 1) {

                            schemaName = aryColumnName[0];
                            columnName = aryColumnName[1];

                        }

                        let objArg = { name: aryArgs[0], value: '', sep: '==' };
                        objArg.type = this.schemas.layerize[schemaName].columns[columnName].type;
                        if (aryArgs.length > 1) {

                            aryArgs.shift();
                            let value = aryArgs.join(':');
                            if (value !== '') {

                                let fval = value.substring(0, 1);
                                let sval = (value.length > 1) ? value.substring(1, 2) : '';
                                let lval = value.substring(value.length - 1);
                                switch (fval) {

                                    case '<':
                                        if (sval === '=') {

                                            objArg.value = value.substring(2);
                                            objArg.sep = '<=';

                                        } else {

                                            objArg.value = value.substring(1);
                                            objArg.sep = '<<';

                                        }
                                        break;
                                    case '>':
                                        if (sval === '=') {

                                            objArg.value = value.substring(2);
                                            objArg.sep = '>=';

                                        } else {

                                            objArg.value = value.substring(1);
                                            objArg.sep = '>>';

                                        }
                                        break;
                                    case '#':
                                        objArg.value = value.substring(1);
                                        objArg.sep = '=#';
                                        break;
                                    case '!':
                                        objArg.sep = '!=';
                                        switch (sval) {

                                            case '=':
                                                objArg.value = value.substring(2);
                                                break;
                                            case '^':
                                                objArg.sep = '!^';
                                                objArg.value = '';
                                                break;
                                            default:
                                                objArg.value = value.substring(1);

                                        }

                                        break;
                                    case '^':
                                        objArg.sep = '^^';
                                        objArg.value = '';
                                        break;
                                    case '?':
                                        objArg.sep = '??';
                                        objArg.value = value.substring(1);
                                        break;
                                    case '%':
                                        if (lval === '%' && value.length > 1) {

                                            objArg.sep = '%%';
                                            objArg.value = value.substring(1, value.length - 1);

                                        } else {

                                            objArg.sep = '%a';
                                            objArg.value = value.substring(1);

                                        }
                                        break;
                                    default:
                                        if (lval === '%' && value.length > 1) {

                                            objArg.sep = 'a%';
                                            objArg.value = value.substring(0, value.length - 1);

                                        } else {

                                            objArg.sep = '==';
                                            objArg.value = value;

                                        }
                                        break;

                                }

                            }

                        }
                        objFilter.columns.push(objArg);
                        objFilter.structure.push('arg');
                        break;

                }

            }

            return { objFilter };

        } catch (error) {

            throw this.error.handle({ error, caller: '__buildFilter' });

        }

    }

    /**
     * A private internal method for cleaning sql to prevent sql injection.
     * @access private
     * @param {string} str - text needing escaping
     * @returns {string} returns cleaned str
     */
    __clean (str = '') {

        str = String(str).replace(/\'/ig, '\'\'');
        while (str.indexOf('\\\\') > -1) {

            str = str.replace(/\\\\/ig, '\\');

        }
        return str;

    }

    /**
     * A private internal method for updating the elasticsearch index and caches on non-transaction insert, patches and updates.
     * @access private
     * @param {string} options.table - name of the table
     * @param {string} [options.type=update] - type of action, can be 'insert' or 'update'
     * @param {array} [options.keys=[]] - array of keys to update
     * @param {schema} options.schema - JSON schema defintion
     * @param {knex} options.statement - a knex instance of the db
     * @param {string|array} [options.fields=*] - fields to be returned
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {string|array} [options.includes=''] - adds other records into return object
     * @param {object} [options.authorization={}] - authorization object
     * @param {array} [options.auditLogs=[]] - audit logs to be saved
     * @returns {Promise<array>} If returnRecord is 'true', then it will return the array of reocrd objects
     * @returns {Promise<success>} If returnRecord is 'false', then it will return a success object
     */
    async __nonTransactionESCacheProcessing ({ type = 'insert', table, keys = [], schema, statement, returnRecords = false, fields = '*', includes = '', authorization, auditLogs = [], layer = this.transaction() } = {}) {

        this.debug(`__nonTransactionProcessing(${table})`, type);
        try {

            let results = {};

            if (schema.esEnabled || returnRecords || (type === 'update' && keys.length === 0)) {

                let items = [];
                let disableCacheResave = false;
                if (!Array.isArray(statement)) {

                    statement.returning('*');

                    let response = await layer.commit({ transactions: [ statement.toSQL() ] });
                    items = response.results[0].rows;

                } else {

                    await layer.commit({ transactions: statement });

                    let cacheKeys = [];
                    for (let i = 0; i < keys.length; i++) {

                        let key = keys[i];
                        cacheKeys.push(this.cache.key(this.cacheKey, table, key));

                    }
                    await this.cache.clear(...cacheKeys);

                    items = await this.getMany(table, keys);

                    // getMany already repopulates the cache.
                    disableCacheResave = true;

                }

                let esTransaction = [];
                let cachebulk = [];
                for (let x = 0; x < items.length; x++) {

                    let item = items[x];

                    if (schema.esEnabled) {

                        esTransaction.push({ index: { _index: `${this.schemaName}~${table}`, _type: 'index', _id: item[schema.primaryKey] } });
                        esTransaction.push(item);

                    }

                    if (!disableCacheResave) {

                        cachebulk.push({
                            key: this.cache.key(this.cacheKey, table, item[schema.primaryKey]),
                            value: item
                        });

                    }

                }

                if (cachebulk.length > 0 && !disableCacheResave) {

                    await this.cache.mset(cachebulk, this.cacheExpireSeconds);

                }

                if (esTransaction.length > 0) {

                    let esResponse = await this.es.bulk({ refresh: true, body: esTransaction });

                    if (esResponse.items.length > 0) {

                        let es = {
                            index: esResponse.items[0].index._index,
                            type: 'index',
                            from: 0,
                            size: 100,
                            body: {
                                query: {
                                    match_all: {}
                                }
                            }
                        };

                        esResponse = await this.es.search(es);

                    }

                    if (esResponse.errors) {

                        error(JSON.stringify(esResponse));

                    }

                }

                if (returnRecords === false) {

                    results = { success: true };

                } else {

                    ({ results } = await this._includes({ table, results: items, includes }));
                    results = requestFields.parse(results, { stacked: [...authorization.properties, fields] });

                }

            } else {

                results = await layer.commit({ transactions: [ statement.toSQL() ] });
                results = { success: true };

                if (type === 'update') {

                    let cacheKeys = [];
                    for (let i = 0; i < keys.length; i++) {

                        let key = keys[i];
                        cacheKeys.push(this.cache.key(this.cacheKey, table, key));

                    }
                    await this.cache.clear(...cacheKeys);

                }

            }

            await this._runWatcher({ tables: [ { table, on: type, keys } ] });

            if (auditLogs.length > 0) {

                await this._saveAuditLogs({ logs: auditLogs });

            }

            return results;

        } catch (error) {

            throw this.error.handle({ error, caller: '__nonTransactionProcessing' });

        }

    }

    /**
     * A private internal method for removing multiple spaces from a string.
     * @access private
     * @param {string} str - text needing escaping
     * @returns {string} returns cleaned str
     */
    __removeMultipleSpaces (str) {

        return str.replace(/ +(?= )/g, '');

    }

    /**
     * A private internal method for removing all spaces from a string.
     * @access private
     * @param {string} str - text needing escaping
     * @returns {string} returns cleaned str
     */
    __removeAllSpaces (str) {

        return (String(str)).replace(/ /g, '');

    }

    /**
     * A private internal method for converting the data object into a way the database can accept.
     * @access private
     * @param {object=} options - available options
     * @param {string} options.table - name of the table
     * @param {string} [options.type=update] - type of action, can be 'insert' or 'update'
     * @param {object} [options.data={}] - data to be validated
     * @param {schema=} [options.schema] - JSON schema defintion, if not passed it uses table default schema
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @returns {Promise<object>} returns an object containing the passed in data parameter
     */
    async __convertForDatabase ({ type = 'update', table = '', data = {}, schema = {}, ignoreReadOnly = [] } = {}) {

        this.debug('__convertForDatabase()');
        try {

            data = objects.clone(data);

            let objTable = this.schemas.layerize[table];

            for (let property in data) {

                let column = objTable.columns[property];

                if (typeof column === 'undefined' || (column.readOnly === true && ignoreReadOnly.indexOf(property) === -1 && !(type === 'insert' && column.definableOnCreate === true))) {

                    delete data[property];

                } else {

                    if (column.type === 'array' || column.type === 'object' || column.type === 'jsonb') {

                        if (typeof data[property] !== 'string') {

                            data[property] = JSON.stringify(data[property]);

                        }

                    }

                }

            }

            for (let i = 0; i < schema.timestamps.length; i++) {

                let column = schema.timestamps[i];
                data[column.name] = 'now()';

            }

            return { data };

        } catch (error) {

            throw this.error.handle({ error, caller: '__convertForDatabase' });

        }

    }

    /**
     * A private internal method for turning strings in to arrays based on type.
     * @access private
     * @param {string} str - text needing escaping
     * @param {number} type - (0) split by |<br> (1) split by :<br> (2) split by () && ||<br> (3) split by space<br> (4) split by comma
     * @returns {array} returns array from split string
     */
    __toArray (str, type = 4) { //string into array

        switch (type) {

            case 1: // :
                // return str.split(/(?:[^[:]]|:)+/g);
                return str.split(/:/g);
            case 2: // () && ||
                return str.split(/([()]|&&|\|\|)/).filter((x) => x);
            case 3: //space
                str = this.__removeMultipleSpaces(str);
                return str.split(' ');
            case 4: //comma
                str = this.__removeAllSpaces(str);
                return str.split(',');

        }

    }

}

module.exports = Layers;
