'use strict';

const Ajv = require('ajv');
const ajvAsync = require('ajv-async');
const loadKeywords = require('../keywords');
const loadFormats = require('../formats');

const ajv = new Ajv({ allErrors: true, useDefaults: true, coerceTypes: true });

ajvAsync(ajv);
loadKeywords(ajv);
loadFormats(ajv);

module.exports = ajv;
