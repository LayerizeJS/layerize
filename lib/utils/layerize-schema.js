'use strict';

const extend = require('extend');
const errors = require('./errors');
const debug = require('debug')('layerize:utils:layerize-schema');
const _ = require('lodash');

const { pick } = _;

const schemaParser = {
    parseRefs: ({ jsonSchema = {} }) => {

        let parsed = extend(true, {}, jsonSchema);

        /**
         * private internal function for parseRefs
         * @access private
         * @param {object} schema - schema
         * @param {object} refs - refs
         * @param {object} refVal - refVal
         * @param {object} id - id
         * @param {numeric} depth - depth of recursive
         * @returns {object} schema
         */
        let scanProperties = (schema, refs, refVal, id, depth = 0) => {

            if (depth < 20) {

                for (var property in schema) {

                    if (schema.hasOwnProperty(property)) {

                        let value = schema[property];
                        if (property === '$ref') {

                            if (value.charAt(0) === '#') {

                                value = `${id}${value}`;

                            }

                            let refObj = refVal[refs[value]];
                            let newObj = {};

                            if (typeof refObj.schema !== 'undefined' && typeof refObj.refs !== 'undefined' && typeof refObj.refVal !== 'undefined') {

                                let parsedSchema = parsed.schema || {};
                                newObj = scanProperties(refObj.schema || {}, refObj.refs || {}, refObj.refVal || [], parsedSchema.$id || '', depth + 1);

                            } else {

                                newObj = scanProperties(refObj || {}, refs, refVal, id, depth + 1);

                            }

                            schema = extend(true, schema, newObj);

                            delete schema[property];

                        }

                        if (typeof value === 'object') {

                            if (Array.isArray(value)) {

                                for (let x = 0; x < value.length; x++) {

                                    value[x] = scanProperties(value[x], refs, refVal, id, depth + 1);

                                }

                            } else {

                                value = scanProperties(value, refs, refVal, id, depth + 1);

                            }

                        }

                    }

                }

            }

            return schema;

        };

        let schema = null;
        let parsedSchema = parsed.schema || {};

        if (parsedSchema.$root) {

            schema = scanProperties(parsedSchema, parsed.refs || {}, parsed.refVal || [], parsedSchema.$id || '');

            if (schema.type !== 'object') {

                throw new Error('schemas that have $root:true must have the \'type\' property set to \'object\'');

            }

        }

        return schema;

    },
    allowedProperties: ({ schema = {}, validator } = {}) => {

        debug('allowedProperties()');

        try {

            const allowedProperties = {
                root: [],
                embeddedArrays: {}
            };

            /**
             * recurvise
             * @param {object} properties - a file pattern to schema locations
             * @param {string} parentPropertyName - a string of the parent property name
             * @param {boolean} depth - current depth of the recursive
             * @param {boolean} parentIsArray - is the parent an array
             * @returns {array}  list of allowed properties
             */
            const recursive = (properties, parentPropertyName = '', depth = 0, parentIsArray = false) => {

                const allowed = [];

                if (depth < 20) {

                    for (let propertyName in properties) {

                        if (properties.hasOwnProperty(propertyName)) {

                            const property = properties[propertyName];
                            let propertyPath = propertyName;

                            if (parentPropertyName !== '' && !parentIsArray) {

                                propertyPath = `${parentPropertyName}.${propertyName}`;

                            }

                            switch (property.type) {

                                case 'object':

                                    if (typeof property.properties === 'object' && property.properties !== null && !Array.isArray(property.properties) && Object.keys(property.properties) > 0) {

                                        const allowedChildren = recursive(property.properties, propertyPath, depth + 1);

                                        if (allowedChildren.length > 0) {

                                            allowed = allowed.concat(allowedChildren);

                                        } else {

                                            allowed.push(propertyPath);

                                        }

                                    } else {

                                        allowed.push(propertyPath);

                                    }
                                    break;

                                case 'array':

                                    allowed.push(propertyPath);

                                    let items = property.items;

                                    if (parentIsArray) {

                                        if (typeof items !== 'undefined' && typeof items.$ref !== 'undefined' && typeof validator !== 'undefined') {

                                            items = validator.getSchema(items.$ref);

                                        }

                                    }

                                    if (typeof items !== 'undefined' && typeof items.schema !== 'undefined') {

                                        items = items.schema;

                                    }

                                    if (typeof items === 'object' && typeof items.properties === 'object' && Object.keys(items.properties).length > 0) {

                                        let arrayPropertyPath = propertyName;

                                        if (parentPropertyName !== '' && parentIsArray) {

                                            arrayPropertyPath = `${parentPropertyName}[*].${propertyName}`;

                                        }

                                        allowedProperties.embeddedArrays[arrayPropertyPath] = recursive(items.properties, arrayPropertyPath, depth + 1, true);

                                    }

                                    break;

                                default:

                                    allowed.push(propertyPath);

                            }

                        }

                    }

                }

                return allowed;

            };

            allowedProperties.root = recursive(schema.properties);

            return allowedProperties;

        } catch (error) {

            throw errors.handle({ error, caller: 'allowedProperties' });

        }

    },
    removeAdditionalProperties: ({ data = {}, allowedProperties } = {}) => {

        debug('removeAdditionalProperties()');

        try {

            if (typeof allowedProperties === 'undefined') {

                throw new errors.Error({ message: 'passing a allowedProperties object is required when using removeAdditionalProperties ()' });

            }

            /**
             * private internal function for parseRefs
             * @access private
             * @param {object} data - data
             * @param {array} allowed - allowedProperties
             * @param {string} parentPath - parentPath
             * @returns {object} schema
             */
            const recursive = (data = {}, allowed = [], parentPath = '') => {

                data = pick(data, allowed);

                for (let propertyName in data) {

                    if (data.hasOwnProperty(propertyName)) {

                        if (typeof allowedProperties.embeddedArrays[propertyName] !== 'undefined') {

                            for (let i = 0; i < data[propertyName].length; i++) {

                                let propertyPath = propertyName;

                                if (parentPath !== '') {

                                    propertyPath = `${parentPath}[*].${propertyName}`;

                                }

                                data[propertyName][i] = recursive(data[propertyName][i], allowedProperties.embeddedArrays[propertyPath], propertyPath);

                            }

                        }

                    }

                }

                return data;

            };

            return recursive(data, allowedProperties.root);

        } catch (error) {

            throw errors.handle({ error, caller: 'allowedProperties' });

        }

    }
};

module.exports = schemaParser;
