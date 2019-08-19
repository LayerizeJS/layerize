'use strict';

module.exports = (ajv) => {

    ajv.addFormat('text', /^(.*?)$/);

    return ajv;

};
