'use strict';
/* eslint-disable max-len, new-cap, camelcase*/

const Base = require('./base');
const Layers = require('./layers');

class Crud extends Base {

    constructor (config = {}) {

        super(config);

    }

    async delete (key = '', query = {}) {

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
            await this._get(key, { transaction }, query);

            /**
             * delete record
             * */
            await this._delete(key, { transaction }, query);

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

    async get (key, query = {}) {

        this.debug('get', query);
        try {

            return await this._get(key, query);

        } catch (error) {

            throw this.error.handle({ error, caller: 'get' });

        }

    }

    async insert (data = {}, query = {}) {

        this.debug('insert');

        try {

            let result = await this._insert(data, { returnRecord: true }, query);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: 'insert' });

        }

    }

    async search (query = {}) {

        this.debug('search', query);
        try {

            let result = await this._search(query);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: 'search' });

        }

    }

    async update (key = '', data = {}, query = {}) {

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
            await this._get(key, { transaction }, query);

            /**
             * update record
             * */
            await this._update(key, data, { transaction }, query);

            /**
             * apply changes and release record locks
             * */
            await transaction.commit();

            /**
             * grab updated record
             * */
            let result = await this._get(key, query);

            return result;

        } catch (error) {

            /**
             * be sure to rollback if there is an error
             */
            await transaction.rollback();

            throw this.error.handle({ error, caller: 'update' });

        }

    }

    async _delete (key = '', { transaction = null } = {}, query) {

        this.debug('_delete');
        try {

            if (key === '') {

                throw new Error('a record key is required to update a record');

            }

            let layers = this.layers;

            if (transaction instanceof Layers) {

                layers = transaction;
                query.forUpdate = true;

            }

            ({ key, query, layers } = await this.__additionalValidation({ action: 'delete', key, query, layers }));

            let result = await layers.delete(this.table, key, query);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_delete' });

        }

    }

    async _get (key, { transaction = null } = {}, query = {}) {

        this.debug('_get');

        try {

            let layers = this.layers;

            if (transaction instanceof Layers) {

                layers = transaction;
                query.forUpdate = true;

            }

            let result = await layers.get(this.table, key, query);

            //add requested includes
            result = await this._includes(result, query);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_get' });

        }

    }

    async _includes (result, query = {}) {

        this.debug('_includes', query);
        return result;

    }

    async _insert (data = {}, { transaction = null, returnRecord = true, ignoreReadOnly = [] } = {}, query = {}) {

        this.debug('_insert');
        try {

            let layers = this.layers;

            if (transaction instanceof Layers) {

                layers = transaction;
                query.forUpdate = true;

            }

            ({ data, query, layers, returnRecord } = await this.__additionalValidation({ action: 'insert', data, query, layers, returnRecord }));

            query.returnRecord = returnRecord;
            query.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.insert(this.table, data, query);

            if (returnRecord) {

                result = await this._includes(result, query);

            }

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_insert' });

        }

    }

    async _search (query = {}) {

        this.debug('_search');

        try {

            let result = await this.layers.search(this.table, query);

            //add requested includes
            result = await this._includes(result, query);

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_search' });

        }

    }

    async _update (key = '', data = {}, { transaction = null, returnRecord = true, ignoreReadOnly = [] } = {}, query = {}) {

        this.debug('_update');
        try {

            if (key === '') {

                throw new Error('a record key is required to update a record');

            }

            if (Object.keys(data).length === 0) {

                throw new Error('data object is empty and there is nothing to update');

            }

            let layers = this.layers;

            if (transaction instanceof Layers) {

                layers = transaction;
                query.forUpdate = true;

            }

            ({ key, data, query, layers, returnRecord } = await this.__additionalValidation({ action: 'update', key, data, query, layers, returnRecord }));

            query.returnRecord = returnRecord;
            query.ignoreReadOnly = ignoreReadOnly;

            let result = await layers.patch(this.table, key, data, query);

            if (returnRecord) {

                result = await this._includes(result, query);

            }

            return result;

        } catch (error) {

            throw this.error.handle({ error, caller: '_update' });

        }

    }

    async __additionalValidation ({ action = 'insert', key = '', data = {}, query = {}, returnRecord = true, layers = null } = {}) {

        this.debug('__additionalValidation', action);
        try {

            return { key, data, query, returnRecord, layers };

        } catch (error) {

            throw this.error.handle({ error, caller: '__additionalValidation' });

        }

    }

}

module.exports = Crud;
