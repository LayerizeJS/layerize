'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:tables');
const extend = require('extend');
const Crud = require('./crud');

class Tables extends Crud {

    constructor (config = {}) {

        super(extend(true, config, {
            table: 'tables',
            debug
        }));

    }

}

module.exports = Tables;
