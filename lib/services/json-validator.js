'use strict';
/* eslint-disable new-cap, camelcase*/

const Ajv = require('ajv');
const ajvAsync = require('ajv-async');
const loadKeywords = require('../keywords');
const loadFormats = require('../formats');

/**
 * The JsonValidator class provides a preconfigured ajv instance.
 */
class JsonValidator {

    /**
     * Create a JsonValidator.
     * @param {object} config - available options
     * @param {boolean} [config.allErrors=true] - return all errors.
     * @param {boolean} [config.useDefaults=true] - use defaults.
     * @param {boolean} [config.coerceTypes=false] - Ajv coerceTypes.
     */
    constructor ({ allErrors = true, useDefaults = true, coerceTypes = false, removeAdditional = true, layerizeEncryptionKey = '', utils } = {}) {

        this.validator = new Ajv({ allErrors, useDefaults, coerceTypes, removeAdditional, schemaId: 'auto' });

        ajvAsync(this.validator);
        loadKeywords(this.validator, { utils, layerizeEncryptionKey });
        loadFormats(this.validator, { utils });

    }

}

module.exports = JsonValidator;
