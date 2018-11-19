'use strict';
/* eslint-disable global-require */

const path = require('path');
const fs = require('fs');
const _ = require('lodash');

const files = fs.readdirSync(path.join(__dirname));
files.forEach(file => {

    const fN = file.replace(/\.[^/.]+$/, '');

    if (fN !== 'index') {

        let methodName = _.lowerFirst(_.camelCase(fN));
        module.exports[`${methodName}`] = require(`./${fN}`);

    }

});
