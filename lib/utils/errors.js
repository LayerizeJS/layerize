'use strict';

const debug = require('debug')('layerize:utils:errors');

let errors = {

    /**
     * Formats the error into a JSON friendly response
     * @param {object} options - options
     * @param {error} error - error
     * @param {string} caller - error caller
     * @returns {Promise<object>} elasticsearch body object
     */
    handle: ({ error, caller = '' } = {}) => {

        if (typeof error !== 'object' || error instanceof Error) {

            let message = 'Unable to get error message';
            let line = 0;
            let code = 0;
            let statusCode = 500;
            let details = {};
            let more_info = '';
            if (typeof error !== 'undefined') {

                message = error.detail || error.message;
                line = parseInt(error.line || '0', 10);
                code = error.code || '0';
                more_info = error.more_info || '';

                if (typeof message === 'undefined') {

                    message = error.error;

                }

                if (typeof error.details === 'object' && !Array.isArray(error.details) && error.details !== null) {

                    details = error.details;

                }

                if (Object.keys(details).length === 0 && (typeof error.schema !== 'undefined' || typeof error.table !== 'undefined' || typeof error.routine !== 'undefined')) {

                    details = {
                        schema: error.schema,
                        table: error.table,
                        routine: error.routine
                    };

                }

                if (Array.isArray(error.errors)) {

                    details.errors = error.errors;

                }

                if (typeof error.statusCode === 'undefined') {

                    if (String(code).startsWith('23') && String(code).length === 5) {

                        statusCode = 400;

                    } else {

                        statusCode = 500;

                    }

                } else {

                    statusCode = error.statusCode || 500;

                }

            }

            debug(error);
            error = { message, caller, line, code, statusCode, details, more_info };

        }
        debug(`${caller} ERROR: ${JSON.stringify(error)}`);
        return error;

    },

    /**
     * Custom Error extends Error
     * @param {object} options - options
     * @param {string} message - error
     * @param {integer} code - method name
     * @returns {Promise<object>} elasticsearch body object
     */
    Error: function ({ message = '', code = '0', statusCode = 500, details = {}, moreInfo = '' } = {}) {

        let error = Error.call(this, message);

        this.name = 'CustomError';
        this.message = error.message;
        this.stack = error.stack;
        this.code = code;
        this.statusCode = statusCode;
        this.details = details;
        this.moreInfo = moreInfo;

    }

};

errors.Error.prototype = Object.create(Error.prototype);
errors.Error.prototype.constructor = errors.Error;

module.exports = errors;
