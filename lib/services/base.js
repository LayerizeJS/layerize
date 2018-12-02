'use strict';
/* eslint-disable new-cap, camelcase*/

const defaultDebug = require('debug')('layerize:base');
const { errors } = require('../utils');

class Base {

    constructor ({ schemaName = 'public', layerize = null, table = '', debug = defaultDebug } = {}) {

        /**
         * CONFIGURE
         */
        this.schemaName = schemaName;
        this.layerize = layerize;
        this.layers = this.layerize.layers({ schemaName: this.schemaName });
        this.table = table;
        this.debug = debug;
        this.error = errors;

    }

}

module.exports = Base;
