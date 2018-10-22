'use strict';

const mask = require('json-mask');

module.exports = function (data, fields) {

    return mask(data, fields);

};
