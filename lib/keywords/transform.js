'use strict';

let crypto = require('crypto');

module.exports = (ajv, { utils, layerizeEncryptionKey }) => {

    /**
     * private internal function for dynamicDefaults
     * @access private
     * @param {string} value - schema
     * @returns {string} value that has been lowercased
     */
    let makeHashTableKey = value => value.toLowerCase();

    let transform = {
        trimLeft: value => value.replace(/^[\s]+/, ''),
        trimRight: value => value.replace(/[\s]+$/, ''),
        trim: value => value.trim(),
        toLowerCase: value => value.toLowerCase(),
        toUpperCase: value => value.toUpperCase(),
        toEnumCase: (value, cfg) => cfg.hash[makeHashTableKey(value)] || value,
        toMD5: value => {

            if (typeof value === 'undefined' || value === '') {

                return value;

            }

            let md5Reg = /^[a-f0-9]{32}$/i;
            if (md5Reg.test(value)) {

                return value;

            } else {

                return crypto.createHash('md5').update(value).digest('hex');

            }

        },
        toEncrypt: value => {

            if (typeof value === 'undefined' || value === '' || value === null) {

                return value;

            }

            if (utils.crypto.isEncrypted(value, layerizeEncryptionKey)) {

                return value;

            } else {

                return utils.crypto.encrypt(value, layerizeEncryptionKey);

            }

        }
    };

    let definition = {
        type: 'string',
        errors: false,
        modifying: true,
        valid: true,

        /**
         * private internal function for tranform
         * @access private
         * @param {string} schema - schema
         * @param {object} parentSchema - parentSchema
         * @returns {boolean} true
         */
        compile: function (schema, parentSchema) {

            let cfg;

            if (schema === 'toEnumCase') {

                // build hash table to enum values
                cfg = { hash: {} };

                // requires `enum` in schema
                if (!parentSchema.enum) {

                    throw new Error('Missing enum. To use `transform:["toEnumCase"]`, `enum:[...]` is required.');

                }

                for (let i = parentSchema.enum.length; i--; i) {

                    let v = parentSchema.enum[i];

                    if (typeof v !== 'string') {

                        continue;

                    }

                    let k = makeHashTableKey(v);

                    // requires all `enum` values have unique keys
                    if (cfg.hash[k]) {

                        throw new Error('Invalid enum uniqueness. To use `transform:["toEnumCase"]`, all values must be unique when case insensitive.');

                    }

                    cfg.hash[k] = v;

                }

            }

            return (data, dataPath, object, key) => {

                // skip if value only
                if (!object) {

                    return;

                }

                // apply transform in order provided
                for (let j = 0, l = schema.length; j < l; j++) {

                    data = transform[schema[j]](data, cfg);

                }

                object[key] = data;

            };

        },
        metaSchema: {
            type: 'array',
            items: {
                type: 'string',
                enum: [
                    'trimLeft', 'trimRight', 'trim',
                    'toLowerCase', 'toUpperCase', 'toEnumCase',
                    'toMD5', 'toEncrypt'
                ]
            }
        }
    };

    ajv.addKeyword('transform', definition);

    return ajv;

};
