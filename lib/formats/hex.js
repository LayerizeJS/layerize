'use strict';

module.exports = (ajv) => {

    ajv.addFormat('hex', /^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$/i);

    return ajv;

};
