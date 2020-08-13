'use strict';

module.exports = (ajv) => {

    ajv.addFormat('key', /^[a-z0-9_]+$/);

    return ajv;

};
