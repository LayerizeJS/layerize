'use strict';
/* eslint-disable global-require */

const path = require('path');
const fs = require('fs');

module.exports = (ajv) => {

    const files = fs.readdirSync(path.join(__dirname));
    files.forEach(file => {

        const fN = file.replace(/\.[^/.]+$/, '');

        if (fN !== 'index') {

            require(`./${fN}`)(ajv);

        }

    });

};
