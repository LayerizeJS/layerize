'use strict';

module.exports = (ajv) => {

    ajv.addFormat('password', /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])[a-zA-Z0-9!@#$%^&*]{8,}|[a-f0-9]{32}$/);

    return ajv;

};
