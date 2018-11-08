'use strict';

const debug = require('debug')('layerize:layers');
const { errors, requestFields } = require('../utils');

class Layers {

    constructor ({ name, database, cache, search, schemas, validator, cacheExpireSeconds = 60 * 60 * 24 * 7, realTimeTransactions = false, isTransaction = false } = {}) {

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
        this.cacheExpireSeconds = 60 * 60 * 24 * 7; // 7 days
        this.cacheKey = `LAYERIZE:${this.schemaName.toUpperCase()}`;

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
     * Commits the current transaction
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async commit ({ autoRollback = true } = {}) {

        this.debug('commit()');
        try {

            if (this.isTransaction === false) {

                throw new Error('commit can only be called inside a transaction');

            }

            if (this.transactions.length > 0) {

                let trx = await this._createTransaction();

                for (let i = 0; i < this.transactions.length; i++) {

                    let statement = this.transactions[i];
                    await this.database.raw(statement.sql, statement.bindings).transacting(trx);

                }

                try {

                    trx.commit();

                    this.transactions = [];

                } catch (e) {

                    if (autoRollback) {

                        trx.rollback();

                    }

                    throw e;

                }

            }

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

            return await count;

        } catch (error) {

            throw this.error.handle({ error, caller: 'count' });

        }

    }

    /**
     * Deletes a single records from the data layer
     * @param {String} table
     * @param {String} key
     * @param {Object} data
     * @param {Object} options { }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async delete (table = '', key = '', {} = {}) {

        this.debug('delete()');
        try {

            let schema = this.schemas.layerize[table];

            let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).delete();

            const cacheKey = this.cache.key(this.cacheKey, table, key);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.cache = [ cacheKey ];
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let result = await statement;

                await this.cache.clear(cacheKey);

                return result;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'delete' });

        }

    }

    /**
     * Deletes a single or mulitple records from the data layer that is meets the filter restraint
     * @param {String} table
     * @param {String} key
     * @param {Object} data
     * @param {Object} options { }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async deleteByFilter (table = '', filter = '', {} = {}) {

        this.debug('deleteByFilter()');
        try {

            let statement = this.dbSchema.clone().table(table);
            statement = this._applyFilters(statement, { table, filter });
            statement.delete();

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                return await statement;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'deleteByFilter' });

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
     * @param {String} key
     * @param {Object} options { fields = '',  joins = '' }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async get (table = '', key = '', { fields = '', joins = '' } = {}) {

        this.debug('get()', { joins });
        try {

            //See if caching layer has record
            const cacheKey = this.cache.key(this.cacheKey, table, key);
            let result = await this.cache.get(cacheKey);

            //if caching layer does not have record the pull it from the data source
            if (result === null) {

                let schema = this.schemas.layerize[table];

                let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key });

                let results = await statement;

                if (results.length === 0) {

                    throw new Error(`Record '${key}' could not be found in '${table}' table.`);

                }

                result = results[0];

                this.cache.set(cacheKey, this.cacheExpireSeconds, result);

            }

            return requestFields(result, fields);

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

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
     * @param {Object} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [] }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async insert (table = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '', joins = '' } = {}) {

        this.debug('insert()', returnRecord);
        try {

            if (table === '') {

                throw new Error('table param is required when doing an insert');

            }

            let schema = this.schemas.layerize[table];

            if (Array.isArray(data)) {

                for (let i = 0; i < data.length; i++) {

                    data[i] = await this._validate({ table, data: data[i], ignoreReadOnly });

                }

            } else {

                data = await this._validate({ table, data, ignoreReadOnly });

            }

            this.debug('table', table);

            let statement = this.dbSchema.clone().table(table).insert(data);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let result = await statement;

                if (returnRecord) {

                    if (schema.columns[schema.primaryKey].serial === true) {

                        // TODO: add support for returning serial from insert

                    } else {

                        if (Array.isArray(data)) {

                            result = [];
                            for (let i = 0; i < data.length; i++) {

                                result.push(await this.get(table, data[schema.primaryKey], { fields, joins }));

                            }

                        } else {

                            result = await this.get(table, data[schema.primaryKey], { fields, joins });

                        }

                    }

                }

                return result;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'insert' });

        }

    }

    /**
     * Patches a single partial records into the data layer
     * @param {String} table
     * @param {String} key
     * @param {Object} data
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [] }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async patch (table = '', key = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '', joins = '' } = {}) {

        this.debug('patch()');
        try {

            let schema = this.schemas.layerize[table];

            let narrowedSchema = this._narrowedSchema({ schema, properties: Object.keys(data) });

            data = await this._validate({ table, data, ignoreReadOnly, schema: narrowedSchema });

            let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(data);

            const cacheKey = this.cache.key(this.cacheKey, table, key);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.cache = [ cacheKey ];
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let result = await statement;

                await this.cache.clear(cacheKey);

                if (returnRecord) {

                    result = await this.get(table, key, { fields, joins });

                }

                return result;

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

            let schema = this.schemas.layerize[table];

            let narrowedSchema = this._narrowedSchema({ schema, properties: Object.keys(data) });

            data = await this._validate({ table, data, ignoreReadOnly, schema: narrowedSchema });

            let statement = this.dbSchema.clone().table(table);
            statement = this._applyFilters(statement, { table, filter });
            statement.update(data);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                return await statement;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'patchByFilter' });

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

            // let result = await transaction.rollback(); // only needed if realTimeTransactions=true

            // remove record locks

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
        offset = 0
    } = {}) {

        this.debug('search()', { joins, group, aggregates });

        try {

            let schema = this.schemas.layerize[table];

            // clean arg data
            sort = (!sort) ? schema.primaryKey : sort;
            offset = (!offset) ? 0 : Math.floor(Math.abs(offset));
            limit = (!limit) ? 50 : Math.floor(Math.abs(limit));

            if (schema) {}
            // getting search sql statement and setting filter
            let statement = this.dbSchema.clone().table(table);
            statement = this._applyFilters(statement, { table, filter });

            // getting count sql statement
            let countStatement = statement.clone().count(schema.primaryKey);

            // set sort, offset, limit, group and aggregates.
            statement.orderBy(sort).offset(offset).limit(limit);

            this.debug(statement.toString());

            return {
                sort,
                offset,
                limit,
                total: parseInt((await countStatement)[0].count, 0),
                items: requestFields(await statement, fields)
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
     * @param {Object} options { returnRecord = false, ignoreReadOnly = [] }
     * @returns {Promise} - resolves: { success: true } | rejects: Error
     */
    async update (table = '', key = '', data = {}, { returnRecord = false, ignoreReadOnly = [], fields = '', joins = '' } = {}) {

        this.debug('update()');
        try {

            let schema = this.schemas.layerize[table];

            data = await this._validate({ table, data, ignoreReadOnly });

            let statement = this.dbSchema.clone().table(table).where({ [schema.primaryKey]: key }).update(data);

            const cacheKey = this.cache.key(this.cacheKey, table, key);

            if (this.isTransaction) {

                let objSQL = statement.toSQL();
                objSQL.cache = [ cacheKey ];
                this.transactions.push(objSQL);
                return objSQL;

            } else {

                let result = await statement;
                this.debug(' -- update statement complete');
                await this.cache.clear(cacheKey);
                this.debug(' -- cache clear complete');
                if (returnRecord) {

                    result = await this.get(table, key, { fields, joins });
                    this.debug(' -- get statement complete');

                }

                return result;

            }

        } catch (error) {

            throw this.error.handle({ error, caller: 'update()' });

        }

    }

    // let raw = layers.raw();
    // raw.search; // elasticsearch module
    // raw.cache; // ioredis module
    // raw.database; // knex module

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

    _stringToObjectFilters ({ table = '', filter = '' } = {}) {

        this.debug('_stringToObjectFilters()');

        let schema = this.schemas.layerize[table];

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

    async _validate ({ table = '', data = {}, schema = {} } = {}) {

        this.debug('_validate()');
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

                throw e.errors;

            }

            let objTable = this.schemas.layerize[table];

            for (let property in data) {

                let column = objTable.columns[property];

                if (typeof column !== 'undefined' && (column.type === 'array' || column.type === 'object' || column.type === 'jsonb')) {

                    if (typeof data[property] !== 'string') {

                        data[property] = JSON.stringify(data[property]);

                    }

                }

            }

            this.debug(' -- validate() complete');
            return data;

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
