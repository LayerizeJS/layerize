'use strict';
/* eslint-disable max-len, new-cap, camelcase*/

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

        super(config);

    }

    /**
     * Deletes a single record
     * @param {string} key - primary key of the record
     * @returns {Promise<success>} return a success object.
     */
    async delete (key = '') {

        this.debug('delete');

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

        this.debug('get', options);
        try {

            return await this._get(key, options);

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

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
     * @param {object} options - available options
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
     * update records within the table
     * @param {string} key - primary key of the record
     * @param {object} data - record data needing updating
     * @param {object} options - available options
     * @returns {Promise<object>} return a record object.
     */
    async update (key = '', data = {}, options = {}) {

        this.debug('update');

        let transaction = { rollback: async () => true };

        try {

            /**
             * create a transaction so we can lock the record we are wanting to update
             * */
            transaction = this.layers.transaction();

            /**
             * lock record for updating by passing the transaction to it
             * */
            await this._get(key, { transaction }, options);

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
     * A protected internal method for deleting a records within the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes

     * @returns {Promise<object>} return a record object.
     */
    async _delete (key = '', { transaction = null }) {

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
     * A protected internal method for inserting a single record into the class's table
     * @access protected
     * @param {object} data - data that is being inserted
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @returns {Promise<object>} return a record object.
     */
    async _insert (data = {}, { transaction = null, returnRecord = true, ignoreReadOnly = [] } = {}) {

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
     * @param {object} options - available options
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
     * A protected internal method for updating records within the class's table
     * @access protected
     * @param {string} key - primary key of the record
     * @param {object} data - data that is being inserted
     * @param {object} options - available options
     * @param {Layer} [options.transaction=null] - passes in a transaction layer to use, if null then it uses classes
     * @returns {Promise<object>} return a record object.
     */
    async _update (key = '', data = {}, { transaction = null, returnRecord = true, ignoreReadOnly = [] } = {}) {

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
                options.forUpdate = true;

            }

            ({ key, data, options, layers, returnRecord } = await this.__additionalValidation({ action: 'update', key, data, options, layers, returnRecord }));

            options.returnRecord = returnRecord;
            options.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.patch(this.table, key, data, options);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_update' });

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
