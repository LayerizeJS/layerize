'use strict';

module.exports = (ajv) => {

    ajv.addFormat('percentage', /^(([0]{1}(\.\d{1,10})?)|([0-1]{1}(\.[0]{1,10})?))$/);

    return ajv;

};
