'use strict';
/* eslint-disable max-len, new-cap, camelcase*/

const debug = require('debug')('layerize:crud');
const Base = require('./base');
const Layers = require('./layers');

/**
 * The Crud class provides basic crud functionality utilizing the Layers. Designed for extending to use for table specific services.
 * @extends Base
 */
class Crud extends Base {

    /**
     * Create a Crud.
     * @param {object} config - available config
     */
    constructor (config = {}) {

        if (typeof config.debug === 'undefined') {

            config.debug = debug;

        }

        super(config);

    }

    /**
     * Deletes a single record
     * @param {string} key - primary key of the record
     * @returns {Promise<success>} return a success object.
     */
    async delete (key = '') {

        this.debug(`delete key ${key}`);

        let transaction = { rollback: async () => true };

        try {

            /**
             * create a transaction so we can lock the record we are wanting to delete
             * */
            transaction = this.layers.transaction();

            /**
             * lock record for deleting by passing the transaction to it
             * */
            await this._get(key, { transaction });

            /**
             * delete record
             * */
            await this._delete(key, { transaction });

            /**
             * apply changes and release record locks
             * */
            await transaction.commit();

            return { success: true };

        } catch (error) {

            /**
             * be sure to rollback if there is an error
             * */
            await transaction.rollback();

            throw this.error.handle({ error, caller: 'delete' });

        }

    }

    /**
     * Get a single record
     * @param {string} key - primary key of the record
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async get (key, options = {}) {

        this.debug(`get key ${key}`);

        try {

            return await this._get(key, options);

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

        }

    }

    /**
     * Get a single record's audit log
     * @param {string} key - primary key of the record
     * @returns {Promise<object>} return a record object.
     */
    async getAuditLog (key) {

        this.debug(`getAuditLog key ${key}`);

        try {

            let layers = this.layers;

            return await layers.getAuditLog(this.table, key);

        } catch (error) {

            throw this.error.handle({ error, caller: 'getAuditLog' });

        }

    }

    /**
     * Insert a single record
     * @param {object} data - primary key of the record
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async insert (data = {}, options = {}) {

        this.debug('insert');

        try {

            let result = await this._insert(data, { returnRecord: true }, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: 'insert' });

        }

    }

    /**
     * Search records within the table
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
     * @returns {Promise<object>} return a record object.
     */
    async search (options = {}) {

        this.debug('search');
        try {

            let result = await this._search(options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: 'search' });

        }

    }

    /**
     * patch records within the table
     * @param {string} key - primary key of the record
     * @param {object} data - record data needing updating
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async patch (key = '', data = {}, options = {}) {

        this.debug('patch');

        let transaction = { rollback: async () => true };

        try {

            /**
             * create a transaction so we can lock the record we are wanting to patch
             * */
            transaction = this.layers.transaction();

            /**
             * lock record for patching by passing the transaction to it
             * */
            options.currentRecord = await this._get(key, { transaction }, options);

            options.transaction = transaction;

            /**
             * patch record
             * */
            await this._patch(key, data, options);

            /**
             * apply changes and release record locks
             * */
            await transaction.commit();

            /** deleting transaction so a lock does not get put in place */
            delete options.transaction;

            /**
             * grab updated record
             * */
            let result = await this._get(key, options);

            return result;

        } catch (error) {

            /**
             * be sure to rollback if there is an error
             */
            await transaction.rollback();

            throw this.error.handle({ error, caller: 'update' });

        }

    }

    /**
     * patch many records within the table
     * @param {array} data - record data needing updating
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async patchMany (data = [], options = {}) {

        this.debug('patchMany');

        let transaction = { rollback: async () => true };

        try {

            const schema = this.schemas.layerize[this.table];
            let keys = [];

            for (let i = 0; i < data.length; i++) {

                let item = data[i];

                // make sure the primary key exists, if not throw error
                if (typeof item[schema.primaryKey] === 'undefined') {

                    throw new this.errors.Error({ message: 'When using patchMany(), each object in the data array must contain the primary key.' });

                }

                keys.push(item[schema.primaryKey]);

            }

            /**
             * create a transaction so we can lock the record we are wanting to update
             * */
            transaction = this.layers.transaction();

            /**
             * lock records for updating by passing the transaction to it
             * */
            await this._getMany(keys, { transaction }, options);

            /**
             * patch records
             * */
            await this._patchMany(data, { transaction }, options);

            /**
             * apply changes and release record locks
             * */
            await transaction.commit();

            /**
             * grab updated records
             * */
            let result = await this._getMany(keys, options);

            return result;

        } catch (error) {

            /**
             * be sure to rollback if there is an error
             */
            await transaction.rollback();

            throw this.error.handle({ error, caller: 'patchMany' });

        }

    }

    /**
     * update records within the table
     * @param {string} key - primary key of the record
     * @param {object} data - full record needing updating
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async update (key = '', data = {}, options = {}) {

        this.debug('update');

        let transaction = { rollback: async () => true };

        try {

            /**
             * create a transaction so we can lock the record we are wanting to patch
             * */
            transaction = this.layers.transaction();

            /**
             * lock record for patching by passing the transaction to it
             * */
            options.currentRecord = await this._get(key, { transaction }, options);

            /**
             * update record
             * */
            await this._update(key, data, { transaction }, options);

            /**
             * apply changes and release record locks
             * */
            await transaction.commit();

            /**
             * grab updated record
             * */
            let result = await this._get(key, options);

            return result;

        } catch (error) {

            /**
             * be sure to rollback if there is an error
             */
            await transaction.rollback();

            throw this.error.handle({ error, caller: 'update' });

        }

    }

    /**
     * Returns a transaction to do calls against
     * @returns {Layer} a new instance of a layer with transaction set to true
     */
    transaction () {

        return this.layers.transaction();

    }

    /**
     * A protected internal method for deleting a records within the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes

     * @returns {Promise<object>} return a record object.
     */
    async _delete (key = '', { transaction = null } = {}) {

        this.debug('_delete');
        try {

            if (key === '') {

                throw new Error('a record key is required to update a record');

            }

            let options = arguments[1] || {};

            let layers = this.layers;

            if (transaction instanceof Layers) {

                layers = transaction;
                options.forUpdate = true;

            }

            ({ key, options, layers } = await this.__additionalValidation({ action: 'delete', key, options, layers }));

            let result = await layers.delete(this.table, key, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_delete' });

        }

    }

    /**
     * A protected internal method for getting a records from the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {permission} [options.permission={}] - enforces any restrictions
     * @returns {Promise<object>} return a record object.
     */
    async _get (key, { transaction = null } = {}) {

        this.debug('_get');

        try {

            let layers = this.layers;

            let options = arguments[1] || {};

            if (transaction instanceof Layers) {

                layers = transaction;
                options.forUpdate = true;

            }

            let result = await layers.get(this.table, key, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_get' });

        }

    }

    /**
     * A protected internal method for getting many records from the class's table
     * @access protected
     * @param {array} keys - primary keys of record
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {permission} [options.permission={}] - enforces any restrictions
     * @returns {Promise<object>} return a record object.
     */
    async _getMany (keys, { transaction = null } = {}) {

        this.debug('_getMany');

        try {

            let layers = this.layers;

            let options = arguments[1] || {};

            if (transaction instanceof Layers) {

                layers = transaction;
                options.forUpdate = true;

            }

            let result = await layers.getMany(this.table, keys, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_getMany' });

        }

    }

    /**
     * A protected internal method for inserting a single record into the class's table
     * @access protected
     * @param {object} data - data that is being inserted
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @returns {Promise<object>} return a record object.
     */
    async _insert (data = {}, { transaction = null, returnRecord = false, ignoreReadOnly = [] } = {}) {

        this.debug('_insert');

        try {

            let layers = this.layers;

            let options = arguments[1] || {};

            if (transaction instanceof Layers) {

                layers = transaction;
                options.forUpdate = true;

            }

            ({ data, options, layers, returnRecord } = await this.__additionalValidation({ data, options, layers, returnRecord }));

            options.returnRecord = returnRecord;
            options.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.insert(this.table, data, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_insert' });

        }

    }

    /**
     * A protected internal method for searching records within the class's table
     * @access protected
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
     * @returns {Promise<results>} return a results.
     */
    async _search (options = {}) {

        this.debug('_search');

        try {

            let result = await this.layers.search(this.table, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_search' });

        }

    }

    /**
     * A protected internal method for patching records within the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} data - data that is being patched
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @returns {Promise<object>} return a record object.
     */
    async _patch (key = '', data = {}, { transaction = null, returnRecord = false, ignoreReadOnly = [] } = {}) {

        this.debug('_patch');
        try {

            if (key === '') {

                throw new Error('a record key is required to patch a record');

            }

            if (Object.keys(data).length === 0) {

                throw new Error('data object is empty and there is nothing to patch');

            }

            let layers = this.layers;

            let options = arguments[2] || {};

            if (transaction instanceof Layers) {

                layers = transaction;

            }

            ({ key, data, options, layers, returnRecord } = await this.__additionalValidation({ action: 'patch', key, data, options, layers, returnRecord }));

            options.returnRecord = returnRecord;
            options.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.patch(this.table, key, data, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_patch' });

        }

    }

    /**
     * A protected internal method for patching many records within the class's table
     * @access protected
     * @param {array} data - data that is being inserted
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @returns {Promise<object>} return a record object.
     */
    async _patchMany (data = [], { transaction = null, returnRecord = false, ignoreReadOnly = [] } = {}) {

        this.debug('_patchMany');
        try {

            if (data.length === 0) {

                throw new Error('data object is empty and there is nothing to patch');

            }

            const schema = this.schemas.layerize[this.table];
            let keys = [];

            for (let i = 0; i < data.length; i++) {

                let item = data[i];

                // make sure the primary key exists, if not throw error
                if (typeof item[schema.primaryKey] === 'undefined') {

                    throw new this.errors.Error({ message: 'When using _patchMany(), each object in the data array must contain the primary key.' });

                }

                keys.push(item[schema.primaryKey]);

            }

            let layers = this.layers;

            let options = arguments[1] || {};

            if (transaction instanceof Layers) {

                layers = transaction;

            }

            ({ key: keys, data, options, layers, returnRecord } = await this.__additionalValidation({ action: 'patch', key: keys, data, options, layers, returnRecord }));

            options.returnRecord = returnRecord;
            options.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.patchMany(this.table, data, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_patchMany' });

        }

    }

    /**
     * A protected internal method for updating records within the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} data - full data that is being updated
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {array} [options.ignoreReadOnly=[]] - will ignore the readOnly restricting on those fields
     * @param {object} [options.currentRecord={}] - currentRecord to compare to, if not passed and audit logs are turned of then it will pull its own copy
     * @returns {Promise<object>} return a record object.
     */
    async _update (key = '', data = {}, { transaction = null, returnRecord = false, ignoreReadOnly = [], currentRecord = {} } = {}) {

        this.debug('_update');
        try {

            if (key === '') {

                throw new Error('a record key is required to update a record');

            }

            if (Object.keys(data).length === 0) {

                throw new Error('data object is empty and there is nothing to update');

            }

            let layers = this.layers;

            let options = arguments[2] || {};

            if (transaction instanceof Layers) {

                layers = transaction;

            }

            ({ key, data, options, layers, returnRecord } = await this.__additionalValidation({ action: 'update', key, data, options, layers, returnRecord }));

            options.returnRecord = returnRecord;
            options.ignoreReadOnly = ignoreReadOnly;
            options.currentRecord = currentRecord;

            let result = await layers.update(this.table, data, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '__update' });

        }

    }

    /**
     * A private internal method for updating records within the class's table
     * @access private
     * @param {object} options - available options
     * @param {string} [options.action='insert'] - the type of action being done
     * @param {string} [options.key=''] - the primary key
     * @param {object} [options.data={}] - data
     * @param {object} [options.options={}] - options
     * @param {boolean} [options.returnRecords=false] - will return new record, if not a transaction
     * @param {Layer} [options.layers=null] - passes in a layer to use
     * @returns {Promise<object>} return object.
     */
    async __additionalValidation ({ action = 'insert', key = '', data, options, returnRecord, layers } = {}) {

        this.debug('__additionalValidation', action);
        try {

            return { key, data, options, returnRecord, layers };

        } catch (error) {

            throw this.error.handle({ error, caller: '__additionalValidation' });

        }

    }

}

module.exports = Crud;
