'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:tables');
const extend = require('extend');
const Crud = require('./crud');

/**
 * Class representing tables.
 * @extends Crud
 */
class Tables extends Crud {

    /**
     * Create a Table.
     * @param {object} config - available config
     */
    constructor (config = {}) {

        super(extend(true, config, {
            table: 'tables',
            debug
        }));

    }

}

module.exports = Tables;
