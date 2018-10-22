'use strict';
/* eslint-disable max-len */

const isBuffer = require('is-buffer');
const unset = require('unset-value');
const sizeof = require('object-sizeof');
const { isEmpty, isPlainObject, transform, keyBy, uniqBy } = require('lodash');
const nestedProperty = require('nested-property');
const ObjectHash = require('node-object-hash');
const objectHash = ObjectHash();
const difference = require('deep-diff').diff;

const objects = {
    sizeof,
    difference,
    hash: objectHash.hash,
    get: (obj = {}, property = '') => {

        return nestedProperty.get(obj, property);

    },
    set: (obj = {}, property = '', value = '') => {

        return nestedProperty.set(obj, property, value);

    },
    uniqBy,
    keyBy,
    sortBy () {

        let fields = [].slice.call(arguments);
        let nFields = fields.length;

        return (A, B) => {

            let a;
            let b;
            let field;
            let key;
            let reverse;
            let result;
            for (let i = 0, l = nFields; i < l; i++) {

                result = 0;
                field = fields[i];

                key = typeof field === 'string' ? field : field.name;

                a = A[key];
                b = B[key];

                if (typeof field.primer !== 'undefined') {

                    a = field.primer(a);
                    b = field.primer(b);

                }

                reverse = (field.reverse) ? -1 : 1;

                if (a < b) {

                    result = reverse * -1;

                }
                if (a > b) {

                    result = Number(reverse);

                }
                if (result !== 0) {

                    break;

                }

            }
            return result;

        };

    },
    flatten: (target, opts) => {

        opts = opts || {};

        let delimiter = opts.delimiter || '.';
        let maxDepth = opts.maxDepth;
        let output = {};

        function step (object, prev, currentDepth) {

            currentDepth = currentDepth || 1;
            Object.keys(object).forEach(function (key) {

                let value = object[key];
                let isarray = opts.safe && Array.isArray(value);
                let type = Object.prototype.toString.call(value);
                let isbuffer = isBuffer(value);
                let isobject = (type === '[object Object]' || type === '[object Array]');

                let newKey;
                if (!isNaN(parseFloat(key)) && isFinite(key)) {

                    newKey = prev ? `${prev}[${key}]` : key;

                } else {

                    newKey = prev ? `${prev}${delimiter}${key}` : key;

                }

                if (!isarray && !isbuffer && isobject && Object.keys(value).length && (!opts.maxDepth || currentDepth < maxDepth)) {

                    return step(value, newKey, currentDepth + 1);

                }

                output[newKey] = value;

            });

        }

        step(target);

        return output;

    },
    omit: (value, keys) => {

        if (typeof value === 'undefined') {

            return {};

        }

        if (Array.isArray(value)) {

            for (var i = 0; i < value.length; i++) {

                value[i] = objects.omit(value[i], keys);

            }
            return value;

        }

        if (typeof value !== 'object') {

            return value;

        }

        if (typeof keys === 'string') {

            keys = [ keys ];

        }

        if (!Array.isArray(keys)) {

            return value;

        }

        for (var j = 0; j < keys.length; j++) {

            unset(value, keys[j]);

        }

        for (var key in value) {

            if (value.hasOwnProperty(key)) {

                value[key] = objects.omit(value[key], keys);

            }

        }

        return value;

    },
    clean: (object, {
        emptyArrays = true,
        emptyObjects = true,
        emptyStrings = true,
        nullValues = true,
        undefinedValues = true
    } = {}) => {

        return transform(object, (result, value, key) => {

            // Recurse into arrays and objects.
            if (Array.isArray(value) || isPlainObject(value)) {

                value = objects.clean(value, { emptyArrays, emptyObjects, emptyStrings, nullValues, undefinedValues });

            }

            // Exclude empty objects.
            if (emptyObjects && isPlainObject(value) && isEmpty(value)) {

                return;

            }

            // Exclude empty arrays.
            if (emptyArrays && Array.isArray(value) && !value.length) {

                return;

            }

            // Exclude empty strings.
            if (emptyStrings && value === '') {

                return;

            }

            // Exclude null values.
            if (nullValues && value === null) {

                return;

            }

            // Exclude undefined values.
            if (undefinedValues && value === undefined) {

                return;

            }

            // Append when recursing arrays.
            if (Array.isArray(result)) {

                return result.push(value);

            }

            result[key] = value;

        });

    }
};

module.exports = objects;
