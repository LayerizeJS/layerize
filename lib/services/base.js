'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:base');
const { errors } = require('../utils');

class Base {

    constructor (config = {}) {

        /**
         * CONFIGURE
         */
        this.schemaName = config.schemaName || 'public';
        this.layerize = config.layerize || null;
        this.layers = this.layerize.layers({ schemaName: this.schemaName });
        this.table = config.table || '';
        this.debug = config.debug || debug;
        this.error = errors;

    }

}

module.exports = Base;
