'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:schemas');
const extend = require('extend');
const Crud = require('./crud');

class Schemas extends Crud {

    constructor (config = {}) {

        super(extend(true, config, {
            table: 'schemas',
            debug
        }));

    }

}

module.exports = Schemas;
