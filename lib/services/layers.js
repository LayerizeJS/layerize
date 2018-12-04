'use strict';
/* eslint-disable no-throw-literal*/

const debug = require('debug')('layerize:layers');
const { errors, requestFields, redis, objects } = require('../utils');
const { setLock } = redis;
const { error } = console;

class Layers {

    constructor ({ name, database, cache, search, schemas, validator, cacheExpireSeconds = 60 * 60 * 24 * 7, lockTimeoutMilliseconds = 30 * 1000, realTimeTransactions = false, isTransaction = false } = {}) {

        this.isTransaction = isTransaction;
        this.schemaName = name || '';
        this.database = database;
        this.cache = cache;
        this.es = search;
        this.schemas = schemas;
        this.validator = validator;
        this.realTimeTransactions = realTimeTransactions;
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.error = errors;
        this.debug = debug;
        this.transactions = [];
        this.cacheExpireSeconds = cacheExpireSeconds;
        this.lockTimeoutMilliseconds = lockTimeoutMilliseconds;
        this.cacheKey = `LAYERIZE:DATA:${this.schemaName.toUpperCase()}`;
        this.lockKey = `LAYERIZE:LOCK:${this.schemaName.toUpperCase()}`;
        this.activeLocks = [];
        this.esTransaction = [];
        this.hasBeenCombined = false;

        if (this.schemaName !== '') {

            this.dbSchema = this.database.withSchema(this.schemaName).clone();

        } else {

            this.dbSchema = this.database.withSchema('').clone();

        }

    }

    /**
     * Clears all tables caches
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async clearAllTablesCache () {

        this.debug('clearAllTablesCache()');
        try {

            if (this.isTransaction === true) {

                throw new Error('clearAllTablesCache can not be called inside a transaction');

            }

            const cacheKey = this.cache.key(this.cacheKey, '*');
            return await this.cache.deleteByPattern(cacheKey);

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearAllTablesCache' });

        }

    }

    /**
     * Clears a single tables cache
     * @param {String} table
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async clearTableCache (table = '') {

        this.debug('clearTableCache()');
        try {

            if (this.isTransaction === true) {

                throw new Error('clearTableCache can not be called inside a transaction');

            }

            if (table === '') {

                throw new Error('passing a table name is required when using clearTableCache(table)');

            }

            const cacheKey = this.cache.key(this.cacheKey, table, '*');
            return await this.cache.deleteByPattern(cacheKey);

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearTableCache' });

        }

    }

    /**
     * Clears a single record cache
     * @param {String} table
     * @param {String} key
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async clearRecordCache (table = '', key = '') {

        this.debug('clearRecordCache()');
        try {

            if (this.isTransaction === true) {

                throw new Error('clearRecordCache can not be called inside a transaction');

            }

            if (table === '') {

                throw new Error('passing a table name is required when using clearRecordCache(table, key)');

            }

            if (key === '') {

                throw new Error('passing a record key is required when using clearRecordCache(table, key)');

            }

            const cacheKey = this.cache.key(this.cacheKey, table, key);
            return await this.cache.clear(cacheKey);

        } catch (error) {

            throw this.error.handle({ error, caller: 'clearRecordCache' });

        }

    }

    combine (transaction) {

        this.debug('combine');
        try {

            if (this.isTransaction === false) {

                throw new Error('combine can only be called inside a transaction');

            }

            if (this.realTimeTransactions === true) {

                throw new Error('combine can only be used when \'realTimeTransactions\ is set to \'false\'');

            }

            this.activeLocks = this.activeLocks.concat(transaction.activeLocks);
            this.transactions = this.transactions.concat(transaction.transactions);

            this.hasBeenCombined = true;

            return this.transactions;

        } catch (error) {

            throw this.error.handle({ error, caller: 'combine' });

        }

    }

    /**
     * Commits the current transaction
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async commit ({ transactions = [], autoRollback = true } = {}) {

        this.debug('commit()');
        try {

            if (this.isTransaction === false) {

                throw new Error('commit can only be called inside a transaction');

            }

            if (transactions.length > 0) {

                this.transactions = this.transactions.concat(transactions);

            }

            if (this.transactions.length > 0) {

                let trx = await this._createTransaction();
                let cacheToDelete = [];
                let populateToES = [];
                let populateToCache = [];
                let removeFromES = [];

                for (let i = 0; i < this.transactions.length; i++) {

                    let statement = this.transactions[i];

                    let result = await this.database.raw(statement.sql, statement.bindings).transacting(trx);

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

                        for (let x = 0; x < result.rows.length; x++) {

                            let key = result.row[statement.primaryKey];
                            statement.cache.push(this.cache.key(this.cacheKey, statement.table, key));

                        }

                    }

                    if (typeof statement !== 'undefined' && Array.isArray(statement.cache) && statement.cache.length > 0) {

                        cacheToDelete = cacheToDelete.concat(statement.cache);

                    }

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

                } catch (e) {

                    if (autoRollback) {

                        await trx.rollback();

                    }

                    throw e;

                }

            }

            this.transactions = [];

            this.activeLocks.map(unlock => unlock());

            return { success: true };

        } catch (error) {

            throw this.error.handle({ error, caller: 'commit' });

        }

    }

    /**
     * Gets a record count of a table from the data layer
     * @param {String} table
     * @param {String} key
     * @param {Object} options { fields = '',  joins = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async count (table = '', { fields = '*', filter = '', estimated = false } = {}) {

        this.debug('count()');
        try {

            let statement;

            if (estimated === false) {

                statement = this.dbSchema.clone().table(table).count(fields);
                statement = this._applyFilters(statement, { table, filter });

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
     * @param {String} table
     * @param {String|Array} key
     * @param {Object} data
     * @param {Object} options { }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async delete (table = '', key = '', options = {}) {

        this.debug('delete()');
        try {

            return await this.deleteMany(table, [ key ], options);

        } catch (error) {

            throw this.error.handle({ error, caller: 'delete' });

        }

    }

    /**
     * Deletes a single or mulitple records from the data layer that is meets the filter restraint
     * @param {String} table
     * @param {String|Array} filter
     * @param {Object} data
     * @param {Object} options { }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async deleteByFilter (table = '', filter = '', {} = {}) {

        this.debug('deleteByFilter()');
        try {

            const schema = this.schemas.layerize[table];

            let statement = this.dbSchema.clone().table(table);

            statement = this._applyFilters(statement, { table, filter });
            statement.delete();
            statement.returning(schema.primaryKey);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.table = table;
                objSQL.primaryKey = schema.primaryKey;
                objSQL.removeFromES = true;
                objSQL.removeFromCache = true;
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

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'deleteByFilter' });

        }

    }

    /**
     * Deletes a many records by key from the data layer
     * @param {String} table
     * @param {Array} keys
     * @param {Object} data
     * @param {Object} options { }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async deleteMany (table = '', keys = [], {} = {}) {

        this.debug('deleteMany()');
        try {

            const schema = this.schemas.layerize[table];

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

                return result;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'deleteMany' });

        }

    }

    /**
     * Export data from a table
     * @param {String} table
     * @param {Object} options { filter = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async export (table = '', { filter = '' } = {}) {

        this.debug('export()');
        try {

            if (this.isTransaction === true) {

                throw new Error('export can not be called inside a transaction');

            }

            // let result = await layers.export('table', { options });

            return { table, filter }; //delete this line

        } catch (error) {

            throw this.error.handle({ error, caller: 'export' });

        }

    }

    /**
     * Gets a single record from the data layer
     * @param {String} table
     * @param {String|Array} key
     * @param {Object} options { fields = '',  joins = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async get (table = '', key = '', { fields = '', includes = '', forUpdate = false } = {}) {

        this.debug('get()', { includes });
        try {

            let keys = key;
            if (!Array.isArray(key)) {

                keys = [ key ];

            }

            let results = await this.getMany(table, keys, { fields, includes, forUpdate });

            return results[0];

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

        }

    }

    /**
     * Gets a many records by key from the data layer
     * @param {String} table
     * @param {Array} keys
     * @param {Object} options { fields = '',  joins = '', forUpdate = false  }
     * @returns {Promise} - resolves: [ {} ] | rejects: Error
     */
    async getMany (table = '', keys = [], { fields = '', includes = '', forUpdate = false } = {}) {

        this.debug('getMany()');

        let activeLocks = [];

        try {

            if (forUpdate) {

                if (this.isTransaction === false) {

                    throw new Error('forUpdate flag can only be used inside a transaction');

                }

                for (let i = 0; i < keys.length; i++) {

                    let key = keys[i];
                    activeLocks.push(await setLock(this.cache.key(this.lockKey, key), this.lockTimeoutMilliseconds));

                }

            }

            let cacheKeys = [];
            for (let i = 0; i < keys.length; i++) {

                let key = keys[i];
                cacheKeys.push(this.cache.key(this.cacheKey, table, key));

            }

            // See if caching layer has records
            let results = await this.cache.get(cacheKeys);
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

                const schema = this.schemas.layerize[table];

                let statement = this.dbSchema.clone().table(table).whereIn(schema.primaryKey, notFoundInCache);

                let dbResults = await statement;

                if (dbResults.length === 0) {

                    throw new Error(`Records '${notFoundInCache.join(',')}' could not be found in '${table}' table.`);

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

                    throw new Error(`Records '${stillNotFound.join(',')}' could not be found in '${table}' table.`);

                }

            }

            ({ results } = await this._includes({ table, results, includes }));

            let response = requestFields(results, fields);

            this.activeLocks = this.activeLocks.concat(activeLocks);

            return response;

        } catch (error) {

            activeLocks.map(unlock => unlock());
            throw this.error.handle({ error, caller: 'getMany' });

        }

    }

    /**
     * Import data into a table
     * @param {String} table
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async import (table = '') {

        this.debug('import()');
        try {

            if (this.isTransaction === true) {

                throw new Error('import can not be called inside a transaction');

            }

            // let result = await layers.import('table', { options });

            return { table }; //delete this line

        } catch (error) {

            throw this.error.handle({ error, caller: 'import' });

        }

    }

    /**
     * Inserts a single or mulitple records into the data layer
     * @param {String} table
     * @param {Object|Array} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async insert (table = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '' } = {}) {

        this.debug('insert()', returnRecord);
        try {

            if (!Array.isArray(data)) {

                data = [ data ];

            }

            let results = await this.insertMany(table, data, { returnRecords: returnRecord, ignoreReadOnly, fields });

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
     * @param {String} table
     * @param {Array} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async insertMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '', includes = '' } = {}) {

        this.debug('insertMany()');
        try {

            if (table === '') {

                throw new Error('table param is required when doing an insert');

            }

            const schema = this.schemas.layerize[table];

            for (let i = 0; i < data.length; i++) {

                ({ data: data[i] } = await this._validate({ table, data: data[i], ignoreReadOnly }));

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

                this.transactions.push(objSQL);

                return objSQL;

            } else {

                return await this.__nonTransactionESCacheProcessing({ table, schema, statement, returnRecords, fields, includes });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'insertMany' });

        }

    }

    /**
     * Patches a single partial records into the data layer
     * @param {String} table
     * @param {String} key
     * @param {Array} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async patch (table = '', key = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' } = {}) {

        this.debug('patch()');
        try {

            const schema = this.schemas.layerize[table];

            let narrowedSchema = this._narrowedSchema({ schema, properties: Object.keys(data) });

            ({ data } = await this._validate({ table, data, ignoreReadOnly, schema: narrowedSchema }));

            for (let i = 0; i < schema.timestamps.length; i++) {

                let column = schema.timestamps[i];
                data[column.name] = 'now()';

            }

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

                objSQL.table = table;
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let results = await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys: [ key ], schema, statement, returnRecords: returnRecord, fields, includes });
                return results[0];

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'patch' });

        }

    }

    /**
     * Patches a single or mulitple partial records from the data layer that is meets the filter restraint
     * @param {String} table
     * @param {String} filter
     * @param {Object} data
     * @param {Object} options { ignoreReadOnly = [] }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async patchByFilter (table = '', filter = '', data = {}, { ignoreReadOnly = [] } = {}) {

        this.debug('patchByFilter()');
        try {

            const schema = this.schemas.layerize[table];

            let narrowedSchema = this._narrowedSchema({ schema, properties: Object.keys(data) });

            ({ data } = await this._validate({ table, data, ignoreReadOnly, schema: narrowedSchema }));

            let statement = this.dbSchema.clone().table(table);
            statement = this._applyFilters(statement, { table, filter });
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
     * @param {String} table
     * @param {String} key
     * @param {Array} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async patchMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '', includes = '' } = {}) {

        this.debug('patchMany()');
        try {

            const schema = this.schemas.layerize[table];

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

                    throw new Error('When using patchMany(), each object in the data array must contain the primary key.');

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
                ({ data: item } = await this._validate({ table, data: item, ignoreReadOnly, schema: schemaMap[hash] }));

                // update timestamps
                for (let i = 0; i < schema.timestamps.length; i++) {

                    let column = schema.timestamps[i];
                    item[column.name] = 'now()';

                }

                // build seperate update sql statement for each
                let objSQL = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(item).toSQL();

                // get cache key
                let cacheKey = await this.cache.key(this.cacheKey, table, key);

                objSQL.cache = [ cacheKey ];
                statements.push(objSQL);

            }

            if (this.isTransaction) {

                if (schema.esEnabled) {

                    // attach to first record
                    statements[0].table = table;
                    statements[0].populateToES = true;
                    statements[0].keys = keys;

                }

                this.transactions = this.transactions.concat(statements);
                return statements;

            } else {

                return await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys, schema, statement: statements, returnRecords, fields, includes });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'patchMany' });

        }

    }

    async rawCache () {

        this.debug('__rawCache');
        try {

            //

        } catch (error) {

            throw this.error.handle({ error, caller: '__rawCache' });

        }

    }

    async rawDatabase (str = '') {

        this.debug('__rawDatabase');
        try {

            let statement = this.database.raw(str);

            /**
             * if it is a transaction and also not a SELECT statement then return the SQL string statement
             */
            if (this.isTransaction && str.trim().toUpperCase().indexOf('SELECT') !== 0) {

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

    async rawSearch () {

        this.debug('__rawSearch');
        try {

            //

        } catch (error) {

            throw this.error.handle({ error, caller: '__rawSearch' });

        }

    }

    /**
     * Reindex a table in the search layer
     * @param {String} table
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async reindexSearch (table = '') {

        this.debug('reindexSearch()');
        try {

            if (this.isTransaction === true) {

                throw new Error('reindexSearch can not be called inside a transaction');

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
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async rollback () {

        this.debug('rollback()');
        try {

            if (this.isTransaction === false) {

                throw new Error('rollback can only be called inside a transaction');

            }

            // await transaction.rollback(); // only needed if realTimeTransactions=true

            this.activeLocks.map(unlock => unlock());

            return true;

        } catch (error) {

            throw this.error.handle({ error, caller: 'rollback' });

        }

    }

    /**
     * Search a table from the data layer
     * @param {String} table
     * @param {Object} options {
     *          filter: '' || [],
     *          joins: '' || [],
     *          sort: '' || [],
     *          group: '' || [],
     *          aggregates: '' || [],
     *          fields: '' || [],
     *          limit: 25,
     *          offset: 0
     *      }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async search (table = '', {
        filter = '',
        joins = '',
        sort = '',
        group = '',
        aggregates = '',
        fields = '',
        limit = 50,
        offset = 0,
        includes = ''
    } = {}) {

        this.debug('search()', { joins, group, aggregates });

        try {

            const schema = this.schemas.layerize[table];

            // clean arg data
            sort = (!sort) ? schema.primaryKey : sort;
            offset = (!offset) ? 0 : Math.floor(Math.abs(offset));
            limit = (!limit && limit !== 0) ? 50 : Math.floor(Math.abs(limit));

            if (schema) {}
            // getting search sql statement and setting filter
            let statement = this.dbSchema.clone().table(table);
            statement = this._applyFilters(statement, { table, filter });

            // getting count sql statement
            let countStatement = statement.clone().count(schema.primaryKey);

            // set sort, offset, limit, group and aggregates.
            statement.orderBy(sort).offset(offset);

            if (limit > 0) {

                statement.limit(limit);

            }

            this.debug(statement.toString());

            let results = await statement;
            ({ results } = await this._includes({ table, results, includes }));

            return {
                sort,
                offset,
                limit,
                total: parseInt((await countStatement)[0].count, 0),
                items: requestFields(results, fields)
            };

        } catch (error) {

            throw this.error.handle({ error, caller: 'search' });

        }

    }

    /**
     * Returns a transaction to do calls against
     * @param {Object} options { realTime = false }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    transaction ({ realTime = this.realTimeTransactions } = {}) {

        this.debug('transaction()');
        try {

            if (this.isTransaction === true) {

                throw new Error('transaction can not be called inside a transaction');

            }

            return new Layers({
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
     * @param {String} table
     * @param {String} key
     * @param {Object} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async update (table = '', key = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' } = {}) {

        this.debug('update()');
        try {

            const schema = this.schemas.layerize[table];

            ({ data } = await this._validate({ table, data, ignoreReadOnly }));

            for (let i = 0; i < schema.timestamps.length; i++) {

                let column = schema.timestamps[i];
                data[column.name] = 'now()';

            }

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

                objSQL.table = table;
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let results = await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys: [ key ], schema, statement, returnRecords: returnRecord, fields, includes });
                return results[0];

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'update()' });

        }

    }

    /**
     * Update many full records by key into the data layer
     * @param {String} table
     * @param {String} key
     * @param {Array} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [], fields = '', includes = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async updateMany (table = '', data = [], { returnRecords = false, ignoreReadOnly = [], fields = '', includes = '' } = {}) {

        this.debug('updateMany()');
        try {

            const schema = this.schemas.layerize[table];

            /**
             * 1. loop through each record
             *      a. make sure the primary key exists, if not throw error
             *      b. validate data against original Schema
             *      e. return error as soon as one is found
             *      f. update timestamps
             *      g. build seperate update sql statement for each
             *      h. get cache key
             * 2. create transaction if not part of one
             */

            let keys = [];
            let statements = [];
            for (let i = 0; i < data.length; i++) {

                let item = data[i];

                // make sure the primary key exists, if not throw error
                if (typeof item[schema.primaryKey] === 'undefined') {

                    throw new Error('When using updateMany(), each object in the data array must contain the primary key with all other properties.');

                }

                let key = item[schema.primaryKey];

                // storing key for later populating ES
                keys.push(key);

                // validate data against narrowedSchema, return error as soon as one is found
                ({ data: item } = await this._validate({ table, data: item, ignoreReadOnly }));

                // update timestamps
                for (let i = 0; i < schema.timestamps.length; i++) {

                    let column = schema.timestamps[i];
                    item[column.name] = 'now()';

                }

                // build seperate update sql statement for each
                let objSQL = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(item).toSQL();

                // get cache key
                let cacheKey = await this.cache.key(this.cacheKey, table, key);

                objSQL.cache = [ cacheKey ];
                statements.push(objSQL);

            }

            if (this.isTransaction) {

                if (schema.esEnabled) {

                    // attach to first record
                    statements[0].table = table;
                    statements[0].populateToES = true;
                    statements[0].keys = keys;

                }

                this.transactions = this.transactions.concat(statements);
                return statements;

            } else {

                return await this.__nonTransactionESCacheProcessing({ type: 'update', table, keys, schema, statement: statements, returnRecords, fields, includes });

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'updateMany' });

        }

    }

    // let raw = layers.raw();
    // raw.search; // elasticsearch module
    // raw.cache; // ioredis module
    // raw.database; // knex module

    async _attachListToObject ({ table = '', includeId = '', results = [] } = {}) {

        this.debug('_attachListToObject');
        try {

            if (results.length > 0) {

                const objDataMap = {};

                if (typeof this.schemas.includes[table].referenced[includeId] === 'undefined') {

                    throw new Error(`Unable to find include relationship between '${table}' and '${includeId}'.`);

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

    async _attachToObject ({ table = '', includeId = '', results = [] } = {}) {

        this.debug('_attachToObject');
        try {

            if (results.length > 0) {

                const schema = this.schemas.layerize[table];
                const objDataMap = {};

                if (typeof schema.includes[includeId] === 'undefined') {

                    throw new Error(`Unable to find include relationship between '${table}' and '${includeId}'.`);

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
                let items = await this.getMany(attachTable, keys);

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

    _applyFilters (statement, { table = '', filter = '' } = {}) {

        this.debug('_applyFilters');
        try {

            let objFilter = this._stringToObjectFilters({ table, filter });

            if (objFilter.structure.length > 0) {

                statement.whereRaw(this._filter(objFilter));

            }

            return statement;

        } catch (error) {

            throw this.error.handle({ error, caller: '_applyFilters' });

        }

    }

    _clean (str = '') {

        str = String(str).replace(/\'/ig, '\'\'');
        while (str.indexOf('\\\\') > -1) {

            str = str.replace(/\\\\/ig, '\\');

        }
        return str;

    }

    _createTransaction () {

        this.debug('_createTransaction()');
        try {

            return new Promise((resolve) => this.database.transaction(resolve));

        } catch (error) {

            throw this.error.handle({ error, caller: '_createTransaction' });

        }

    }

    _filter (objFilter) {

        this.debug('_filter');
        try {

            let where = '';

            let lngFilters = objFilter.structure.length;
            if (lngFilters === 0) {

                return where;

            }

            if (lngFilters > 0) {

                let cntArg = 0;
                for (let i = 0; i < lngFilters; i++) {

                    let filter = objFilter.structure[i];

                    if (filter === 'arg') {

                        let objArgName;
                        let objArg = objFilter.columns[cntArg];
                        let strArg = '';
                        let objArgValue = this._clean(objArg.value);
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

            throw this.error.handle({ error, caller: '_filter' });

        }

    }

    async _includes ({ table = '', results = [], includes = '' } = {}) {

        this.debug('includes');
        try {

            if (!Array.isArray(includes)) {

                if (includes === '') {

                    includes = [];

                } else {

                    includes = includes.split(',');

                }

            }

            for (let i = 0; i < includes.length; i++) {

                let includeId = includes[i];

                if (typeof this.schemas.includes[table].source[includeId] === 'undefined' && typeof this.schemas.includes[table].referenced[includeId] === 'undefined') {

                    throw new Error(`Unable to find include relationship between '${table}' and '${includeId}'.`);

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

            throw this.error.handle({ error, caller: 'includes' });

        }

    }

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

    async __nonTransactionESCacheProcessing ({ type = 'insert', table, keys = [], schema, statement, returnRecords = false, fields = '', includes = '' } = {}) {

        this.debug('__nonTransactionProcessing', type);
        try {

            let results = {};

            if (schema.esEnabled || returnRecords || (type === 'update' && keys.length === 0)) {

                let items = [];
                let disableCacheResave = false;
                if (!Array.isArray(statement)) {

                    statement.returning('*');

                    items = await statement;

                } else {

                    let transaction = this.transaction();
                    await transaction.commit({ transactions: statement });

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

                    let esResponse = await this.es.bulk({ body: esTransaction });

                    if (esResponse.errors) {

                        error(JSON.stringify(esResponse));

                    }

                }

                if (returnRecords === false) {

                    results = { success: true };

                } else {

                    ({ results } = await this._includes({ table, results, includes }));
                    results = requestFields(items, fields);

                }

            } else {

                results = await statement;
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

            return results;

        } catch (error) {

            throw this.error.handle({ error, caller: '__nonTransactionProcessing' });

        }

    }

    _stringToObjectFilters ({ table = '', filter = '' } = {}) {

        this.debug('_stringToObjectFilters()');

        const schema = this.schemas.layerize[table];

        let objFilter = {
            raw: [],
            columns: [],
            structure: []
        };

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
                    let objArg = { name: aryArgs[0], value: '', sep: '==' };
                    objArg.type = schema.columns[aryArgs[0]].type;
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

        this._validateFilters(objFilter);

        return objFilter;

    }

    async _validate ({ type = 'update', table = '', data = {}, schema = {}, ignoreReadOnly = [] } = {}) {

        this.debug('_validate()');
        try {

            data = objects.clone(data);

            try {

                if (Object.keys(schema).length) {

                    // use passed schema
                    await this.validator.validate(schema, data);

                } else {

                    // use preloaded schema
                    await this.validator.validate(table, data);

                }

            } catch (e) {

                if (Array.isArray(e.errors)) {

                    throw [400, { errors: e.errors }];

                } else {

                    throw e;

                }

            }

            let objTable = this.schemas.layerize[table];

            for (let property in data) {

                let column = objTable.columns[property];

                if (typeof column === 'undefined' || (column.readOnly === true && ignoreReadOnly.indexOf(property) === -1 && (type !== 'insert' && column.definableOnCreate !== true))) {

                    delete data[property];

                } else {

                    if (column.type === 'array' || column.type === 'object' || column.type === 'jsonb') {

                        if (typeof data[property] !== 'string') {

                            data[property] = JSON.stringify(data[property]);

                        }

                    }

                }

            }

            this.debug(' -- validate() complete');
            return { data };

        } catch (error) {

            throw this.error.handle({ error, caller: '_validate' });

        }

    }

    _validateFilters (objFilter, fields, param) {

        if (!param) {

            param = 'filters';

        }

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

                        throw new Error('filters: ' + operator + ' must be followed by an argument.');

                    }

                    //can not be start
                    if (i === 0) {

                        throw new Error('filters: ' + operator + ' can not lead an argument.');

                    }

                    //must follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (!(prevOps === 'arg' || prevOps === ')')) {

                            throw new Error('filters: ' + operator + ' can not lead an argument.');

                        }

                    }

                    //must be followed by arg,(
                    if (lngOps > i + 1) {

                        nextOps = aryOps[i + 1];
                        if (!(nextOps === 'arg' || nextOps === '(')) {

                            throw new Error('filters: ' + operator + ' must be followed by an argument.');

                        }

                    }
                    strTest += '+';
                    break;
                case '(':
                    //can not be end
                    if (lngOps === i + 1) {

                        throw new Error('filters: ' + operator + ' must be followed by an argument.');

                    }

                    //can not follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (prevOps === 'arg' || prevOps === ')') {

                            throw new Error('filters: ' + operator + ' can only follow an operator.');

                        }

                    }
                    strTest += '(';
                    break;
                case ')':
                    //can not be start
                    if (i === 0) {

                        throw new Error('filters: ' + operator + ' can not lead an argument.');

                    }

                    //must follow arg,)
                    if (i > 0) {

                        prevOps = aryOps[i - 1];
                        if (!(prevOps === 'arg' || prevOps === ')')) {

                            throw new Error('filters: ' + operator + ' must follow an argument.');

                        }

                    }
                    strTest += ')';
                    break;
                default:
                    //if not end then must be followed by &&,||,)
                    if (lngOps > i + 1) {

                        nextOps = aryOps[i + 1];
                        if (nextOps === 'arg' || nextOps === '(') {

                            throw new Error('filters: arguments can not be followed by arguments.');

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

            throw new Error('filters: filter arguments "()" not structured properly.');

        }

        //validate fields
        // let lngFields = objFilter.columns.length;
        // for (let i = 0;i < lngFields;i++) {

        //     let fieldValid = false;
        //     let field = objFilter.columns[i].name;
        //     let aryField = field.split('.');
        //     let lngAryField = aryField.length;
        //     if (lngAryField > 1) {

        //         let fieldPart = 0;
        //         if (obj.namespace === aryField[fieldPart]) {

        //             fieldPart++;

        //         }
        //         if (typeof fields[obj.namespace + '.' + aryField[fieldPart]] !== 'undefined') {

        //             let objColumn = fields[obj.namespace + '.' + aryField[fieldPart]];
        //             switch (objColumn.type) {

        //                 case 'jsonb':
        //                     let keyName = aryField[fieldPart + 1];
        //                     for (let x = fieldPart + 2;x < lngAryField;x++) {

        //                         keyName += ',' + aryField[x];

        //                     }
        //                     objFilter.columns[i].name = obj.namespace + '.' + aryField[fieldPart];
        //                     objFilter.columns[i].keyName = keyName;
        //                     fieldValid = true;
        //                     break;
        //                 default:
        //                     objFilter.columns[i].name = field;
        //                     fieldValid = true;

        //             }

        //         } else if (typeof fields[field] !== 'undefined') {

        //             objFilter.columns[i].name = field;
        //             fieldValid = true;

        //         }

        //     } else {

        //         if (typeof fields[obj.namespace + '.' + field] !== 'undefined') {

        //             objFilter.columns[i].name = obj.namespace + '.' + field;
        //             fieldValid = true;

        //         }

        //     }
        //     if (fieldValid === false) {

        //         throw new Error('filters: \'' + field + '\' was not found in the collection');

        //     }

        // }

        return true;

    }

    __removeMultipleSpaces (str) {

        return str.replace(/ +(?= )/g, '');

    }

    __removeAllSpaces (str) {

        return (String(str)).replace(/ /g, '');

    }

    __toArray (str, type) { //string into array

        if (!type) {

            type = 0;

        }
        switch (type) {

            case 0: // |
                return str.match(/(?:[^\|]|\|\|)+/g);
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
            default:
                return [];

        }

    }

}

module.exports = Layers;
