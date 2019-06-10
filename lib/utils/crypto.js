'use strict';

let crypto = require('crypto');

module.exports = {

    /**
     * MD5 Hash
     * @param {string} value - string to be hashed
     * @param {string} [salt=''] - salt to be added to string
     * @returns {string} md5 hash
     */
    md5 (value, salt = '') {

        if (typeof value === 'undefined' || value === '') {

            return value;

        }

        return crypto.createHash('md5').update(value + salt).digest('hex');

    }

};
