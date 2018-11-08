'use strict';

const mask = require('json-mask');

module.exports = function (data, fields) {

    if (Array.isArray(fields)) {

        fields = fields.join(',');

    }

    return mask(data, fields);

};
