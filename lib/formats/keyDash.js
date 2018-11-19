'use strict';

module.exports = (ajv) => {

    ajv.addFormat('keyDash', /^[a-zA-Z0-9_-]+$/i);

    return ajv;

};
