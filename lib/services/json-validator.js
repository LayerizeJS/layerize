'use strict';
/* eslint-disable new-cap, camelcase*/

const Ajv = require('ajv');
const ajvAsync = require('ajv-async');
const loadKeywords = require('../keywords');
const loadFormats = require('../formats');

class JsonValidator {

    constructor ({ allErrors = true, useDefaults = true, coerceTypes = false } = {}) {

        this.validator = new Ajv({ allErrors, useDefaults, coerceTypes });

        ajvAsync(this.validator);
        loadKeywords(this.validator);
        loadFormats(this.validator);

    }

}

module.exports = JsonValidator;
