'use strict';

let extend = require('extend');

let schemaParser = {
    parseRefs: ({ jsonSchema = {} }) => {

        let parsed = extend(true, {}, jsonSchema);

        /**
         * private internal function for parseRefs
         * @access private
         * @param {object} schema - schema
         * @param {object} refs - refs
         * @param {object} refVal - refVal
         * @param {object} id - id
         * @returns {object} schema
         */
        let scanProperties = (schema, refs, refVal, id) => {

            let properties = Object.keys(schema);
            for (let i = 0; i < properties.length; i++) {

                let property = properties[i];
                let value = schema[property];
                if (property === '$ref') {

                    if (value.charAt(0) === '#') {

                        value = `${id}${value}`;

                    }

                    let refObj = refVal[refs[value]];
                    let newObj = {};

                    if (typeof refObj.schema !== 'undefined' && typeof refObj.refs !== 'undefined' && typeof refObj.refVal !== 'undefined') {

                        let parsedSchema = parsed.schema || {};
                        newObj = scanProperties(refObj.schema || {}, refObj.refs || {}, refObj.refVal || [], parsedSchema.$id || '');

                    } else {

                        newObj = scanProperties(refObj || {}, refs, refVal, id);

                    }

                    schema = extend(true, schema, newObj);

                    delete schema[property];

                }

                if (typeof value === 'object') {

                    if (Array.isArray(value)) {

                        for (let x = 0; x < value.length; x++) {

                            value[x] = scanProperties(value[x], refs, refVal, id);

                        }

                    } else {

                        value = scanProperties(value, refs, refVal, id);

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

    }
};

module.exports = schemaParser;
