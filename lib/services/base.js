'use strict';
/* eslint-disable new-cap, camelcase*/

const defaultDebug = require('debug')('layerize:base');
const { errors } = require('../utils');

/**
 * The Base class is a simple class used for extending other classes with the basic members.
 */
class Base {

    /**
     * Create a Base.
     * @param {object} config - available options
     * @param {string} [config.schemaName=public] - schema name of the database.
     * @param {Layer} [config.layerize=null] - an instance of layers.
     * @param {string} [config.table=''] - table name.
     * @param {debug} [config.debug=defaultDebug] - a instance of debug
     */
    constructor ({ schemaName = 'public', layerize = null, table = '', debug = defaultDebug } = {}) {

        /**
         * Name of the schema being used within the layer
         * @member {string}
         * */
        this.schemaName = schemaName;

        /**
         * the instance of layerize
         * @member {Layerize}
         * */
        this.layerize = layerize;

        /**
         * the active layer
         * @member {Layers}
         * */
        this.layers = this.layerize.layers({ schemaName: this.schemaName });

        /**
         * the name of the table
         * @member {string}
         * */
        this.table = table;

        /**
         * The initialized debug instance
         * @member {debug}
         * */
        this.debug = debug;

        /**
         * The error utility
         * @member {object}
         * */
        this.error = errors;

    }

}

module.exports = Base;
