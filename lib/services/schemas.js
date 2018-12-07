'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:schemas');
const extend = require('extend');
const Crud = require('./crud');

/**
 * Class representing schemas.
 * @extends Crud
 */
class Schemas extends Crud {

    /**
     * Create a Schema.
     * @param {object} config - available config
     */
    constructor (config = {}) {

        super(extend(true, config, {
            table: 'schemas',
            debug
        }));

    }

}

module.exports = Schemas;
