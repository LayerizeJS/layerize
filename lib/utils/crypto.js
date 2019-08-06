'use strict';

const crypto = require('crypto');
const Cryptr = require('cryptr');

/**
    This property value is only used to see if the passed encryption value is already encrypted,
    it DOES NOT provide any type of encryption or extra level of security. Changing this value will
    break backwards compatibility, so no reason to change it. It is not a security risk if exposed.
*/
const encryptedIdentifier = '172659b0e1c68484';

const cryptoMethods = {

    /**
     * MD5 Hash
     * @param {string} value - string to be hashed
     * @param {string} [salt=''] - salt to be added to string
     * @returns {string} md5 hash
     */
    md5: (value, salt = '') => {

        if (typeof value === 'undefined' || value === '') {

            return value;

        }

        return crypto.createHash('md5').update(value + salt).digest('hex');

    },

    /**
     * Checks to see if the value is already encrypted
     * @param {string} value - string that may be encypted
     * @param {string} [secret=''] - secret
     * @returns {boolean} is value already encrypted
     */
    isEncrypted: (value, secret = '') => {

        let txt = cryptoMethods.decrypt(value, secret, false);
        let lngEI = encryptedIdentifier.length;

        return Boolean(txt.length > lngEI && txt.slice(0, lngEI) === encryptedIdentifier);

    },

    /**
     * Encrypts the passed value
     * @param {string} value - string to be encypted
     * @param {string} [secret=''] - secret
     * @returns {string} encrypted value
     */
    encrypt: (value, secret = '') => {

        if (typeof value === 'undefined' || value === null || value === '') {

            throw new Error('Encrypt value must not be null, undefined or blank.');

        }

        if (!secret || typeof secret !== 'string') {

            throw new Error('Encrypt key must be a non-0-length string.');

        }

        value = `${encryptedIdentifier}${value}`;

        const cryptr = new Cryptr(secret);

        return cryptr.encrypt(value);

    },

    /**
     * Decrypts the passed value
     * @param {string} value - string to be decypted
     * @param {string} [secret=''] - secret
     * @param {boolean} [removeEncrptedIdentifier=true] - removes the encrypted indentifier
     * @returns {string} decrypted value
     */
    decrypt: (value, secret, removeEncrptedIdentifier = true) => {

        if (typeof value === 'undefined' || value === null || value === '') {

            throw new Error('Decrypt value must not be null, undefined or blank.');

        }

        if (!secret || typeof secret !== 'string') {

            throw new Error('Decrypt key must be a non-0-length string.');

        }

        const cryptr = new Cryptr(secret);
        let strDecrypted = cryptr.decrypt(value);

        let lngEI = encryptedIdentifier.length;
        if (removeEncrptedIdentifier && strDecrypted.slice(0, lngEI) === encryptedIdentifier) {

            return strDecrypted.slice(lngEI, strDecrypted.length);

        } else {

            return strDecrypted;

        }

    }

};

module.exports = cryptoMethods;
