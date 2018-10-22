'use strict';

let extend = require('extend');

module.exports = {
    parseRefs: ({ jsonSchema = {} }) => {

        let parsed = extend(true, {}, jsonSchema);

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

                throw new Error('root type of schema must be an object');

            }

        }

        return schema;

    },

    mergeAllOf: (objSchema = {}) => {

        let schema = extend(true, {}, objSchema);

        for (let property in schema) {

            if (property === 'allOf') {

                for (let i = 0; i < schema[property].length; i++) {

                    schema = schema[property][i];

                }

            }

        }

    }
};
