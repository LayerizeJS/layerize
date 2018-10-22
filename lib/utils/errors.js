'use strict';

const debug = require('debug')('layerize:utils:errors');

module.exports = {

    handle ({ error, caller = '' } = {}) {

        if (!Array.isArray(error)) {

            let errMessage = 'Unable to get error message';
            let errLine = 0;
            if (typeof error !== 'undefined') {

                errMessage = error.message;
                errLine = error.line;

                if (typeof errMessage === 'undefined') {

                    errMessage = error.error;

                }

            }

            debug(error);
            error = [500, { errors: [ { message: `${caller}: ${errMessage}`, line: errLine } ] }];

        }
        debug(`${caller} ERROR: ${JSON.stringify(error)}`);
        return error;

    }

};
