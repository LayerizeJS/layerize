'use strict';

module.exports = (ajv) => {

    ajv.addFormat('decimal', /^[-+]?[0-9]*\.?[0-9]+$/);

    return ajv;

};
