'use strict';

module.exports = (ajv) => {

    let definition = {
        async: true,
        type: 'string',
        validate: () => new Promise(success => success(true)),
        metaSchema: {
            type: 'object',
            properties: {
                table: {
                    type: 'string'
                }
            },
            required: [ 'table' ]
        }
    };

    ajv.addKeyword('existIn', definition);

    return ajv;

};
