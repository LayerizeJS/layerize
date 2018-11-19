'use strict';

let uuid = require('uuid');
let sequences = {};

let DEFAULTS = {
    timestamp: () => Date.now(),
    datetime: () => (new Date()).toISOString(),
    date: () => (new Date()).toISOString().slice(0, 10),
    time: () => (new Date()).toISOString().slice(11),
    random: () => Math.random(),
    randomint: (args) => {

        let limit = args && args.max || 2;
        return () => Math.floor(Math.random() * limit);

    },
    seq: (args) => {

        let name = args && args.name || '';
        sequences[name] = sequences[name] || 0;
        return () => sequences[name]++;

    },
    uuid: () => uuid.v4()
};

module.exports = (ajv) => {

    let getDefault = (d) => {

        let def = DEFAULTS[d];

        if (def) {

            return def;

        }

        throw new Error('invalid "dynamicDefaults" keyword property value: ' + d);

    };

    let definition = {
        compile: (schema, parentSchema, it) => {

            let funcs = {};

            let assignDefaults = (data) => {

                for (let prop in schema) {

                    if (data[prop] === undefined) {

                        data[prop] = funcs[prop]();

                    }

                }

                return true;

            };

            let noop = () => true;

            for (let key in schema) {

                let d = schema[key];
                let func = getDefault(typeof d === 'string' ? d : d.func);
                funcs[key] = func.length ? func(d.args) : func;

            }

            return it.opts.useDefaults && !it.compositeRule ?
                assignDefaults :
                noop;

        },
        DEFAULTS: DEFAULTS,
        metaSchema: {
            type: 'object',
            additionalProperties: {
                type: ['string', 'object'],
                additionalProperties: false,
                required: ['func', 'args'],
                properties: {
                    func: { type: 'string' },
                    args: { type: 'object' }
                }
            }
        }
    };

    ajv.addKeyword('dynamicDefaults', definition);

    return ajv;

};
