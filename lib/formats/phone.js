'use strict';

module.exports = (ajv) => {

    ajv.addFormat('phone', /^$|^\+[1-9]\d{1,14}$/);

    return ajv;

};

