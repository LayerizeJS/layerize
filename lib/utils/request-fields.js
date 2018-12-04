'use strict';

const mask = require('json-mask');

module.exports = function (data, fields) {

    if (Array.isArray(fields)) {

        fields = fields.join(',');

    }

    // The syntax is loosely based on XPath:
    // a,b,c comma-separated list will select multiple fields
    // a/b/c path will select a field from its parent
    // a(b,c) sub-selection will select many fields from a parent
    // a/*/c the star * wildcard will select all items in a field

    // covert all '.' notations to '/' notation referenced above
    fields = fields.replace(/\./g, '/');

    return mask(data, fields);

};
