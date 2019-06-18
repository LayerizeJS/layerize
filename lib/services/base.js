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
     * @param {Layerize} config.layerize - an instance of layers.
     * @param {string} [config.schemaName=public] - schema name of the database.
     * @param {string} [config.table=''] - table name.
     * @param {debug} [config.debug=defaultDebug] - a instance of debug
     */
    constructor ({ layerize, schemaName = 'public', table = '', debug = defaultDebug, permissions = {} } = {}) {

        if (typeof layerize !== 'object' || layerize === null || typeof layerize === 'undefined') {

            throw new Error('A valid instance of Layerize must be passed to the class.');

        }

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
        this.layers = this.layerize.layers({ schemaName: this.schemaName, permissions });

        /**
         * the schemas active in layerize
         * @member {schemas}
         * */
        this.schemas = this.layerize.schemas;

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

        /**
         * Holds read permission object
         * @member {object}
         * */
        this.readPermission = permissions.read || {};

        /**
         * Holds create permission object
         * @member {object}
         * */
        this.createPermission = permissions.create || {};

        /**
         * Holds update permission object
         * @member {object}
         * */
        this.updatePermission = permissions.update || {};

        /**
         * Holds delete permission object
         * @member {object}
         * */
        this.deletePermission = permissions.delete || {};

    }

}

module.exports = Base;
