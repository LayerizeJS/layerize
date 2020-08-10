'use strict';
/* eslint-disable new-cap, camelcase*/

const debug = require('debug')('layerize:rules');
const { errors, objects } = require('../utils');
const operators = require('../operators');

/**
 * The Rules class is a handling rules.
 */
class Rules {

    /**
     * Create a Rules.
     */
    constructor () {

        /**
         * The initialized debug instance
         * @member {debug}
         * */
        this.debug = debug;

        /**
         * The error utility
         * @member {object}
         * */
        this.error = errors;

    }

    /**
     * Run rules against provide data
     * @param {object=} options - available options
     * @param {object=} [options.rules={}] - rules object
     * @param {array=} [options.data=[]] - array of data to be analyzed
     * @param {string=} [options.mode=include] - can be 'include' or 'exclude'
     * @returns {Promise<array>}  returns revised array of data;
     */
    run ({ rules = {}, data = [], mode = 'include' } = {}) {

        this.debug('run');
        try {

            if (Object.keys(rules).length > 0) {

                let aryData = [];
                for (let i = 0; i < data.length; i++) {

                    let bol = this._processRule({ rules, data: data[i] });

                    switch (mode) {

                        case 'include':

                            if (bol === true) {

                                aryData.push(data[i]);

                            }

                            break;

                        case 'exclude':

                            if (bol === false) {

                                aryData.push(data[i]);

                            }
                            break;

                    }

                }

                data = aryData;

            }

            return data;

        } catch (error) {

            throw this.error.handle({ error, caller: 'run' });

        }

    }

    /**
     * build query to hand to data layer
     * @param {object=} options - available options
     * @param {object=} [options.rules={}] - rules object
     * @param {string=} [options.type='db'] - can be 'include' or 'exclude'
     * @returns {Promise<string|object>} returns string when 'db' and object when 'es'
     */
    async query ({ rules = {}, type = 'db', filter } = {}) {

        this.debug('query');
        try {

            switch (type) {

                case 'db':

                    if (typeof filter === 'undefined') {

                        filter = '';

                    } else if (typeof filter !== 'string') {

                        throw new Error('passed filter must be a string when type is set to \'db\'');

                    }

                    if (Object.keys(rules).length > 0) {

                        if (filter !== '') {

                            filter = `(${filter}) AND `;

                        }

                        filter += await this._queryRuleDB({ rules });

                    }
                    break;

                case 'es':

                    filter = null;

                    break;

                default:

                    throw new Error(`Unrecongized data layer type '${type}'.`);

            }

            return filter;

        } catch (error) {

            throw this.error.handle({ error, caller: 'query' });

        }

    }

    /**
     * Process fact against provide data
     * @param {object=} options - available options
     * @param {string=} [options.property=''] - name of property
     * @param {string=} [options.operator='EQUALS'] - operator
     * @param {string=} [options.value=''] - value
     * @param {object=} [options.data={}] - data to be analyzed
     * @returns {boolean} returns true or false
     */
    _processFact ({ property = '', operator = 'EQUALS', value = '', data = {} } = {}) {

        this.debug('_processFact');
        try {

            if (typeof operators[operator] === 'undefined') {

                throw new Error(`Unable to find rules operator '${operator}', when processing fact.`);

            }

            let fact = objects.get(data, property);
            let validated = operators[operator].parse(fact, value);

            return validated;

        } catch (error) {

            throw this.error.handle({ error, caller: '_processFact' });

        }

    }

    /**
     * Process rules against provide data
     * @param {object=} options - available options
     * @param {object=} [options.rules={}] - rules object
     * @param {object=} [options.data={}] - data to be analyzed
     * @returns {boolean} returns true or false
     */
    _processRule ({ rules = {}, data = {} } = {}) {

        this.debug('_processRule');
        try {

            let validated = true;
            switch (rules.type) {

                case 'all':

                    for (let i = 0; i < rules.children.length; i++) {

                        let bol = this._processRule({ rules: rules.children[i], data });

                        if (bol === false) {

                            validated = false;
                            break;

                        }

                    }
                    break;

                case 'any':

                    if (rules.children.length > 0) {

                        let any = false;
                        for (let i = 0; i < rules.children.length; i++) {

                            let bol = this._processRule({ rules: rules.children[i], data });

                            if (bol === true) {

                                any = true;
                                break;

                            }

                        }
                        validated = any;

                    }

                    break;

                case 'fact':

                    validated = this._processFact({ property: rules.property, operator: rules.operator, value: rules.value, data });
                    break;

                default:

                    throw new Error(`Unrecongized rule type '${rules.type}'.`);

            }

            return validated;

        } catch (error) {

            throw this.error.handle({ error, caller: '_processRule' });

        }

    }

    /**
     * Query fact against provide data
     * @param {object=} options - available options
     * @param {string=} [options.property=''] - name of property
     * @param {string=} [options.operator='EQUALS'] - operator
     * @param {string=} [options.value=''] - value
     * @returns {Promise<boolean>} returns true or false
     */
    _queryFactDB ({ property = '', operator = 'EQUALS', value = '' } = {}) {

        this.debug('_queryFactDB');
        try {

            if (typeof operators[operator] === 'undefined') {

                throw new Error(`Unable to find rules operator '${operator}', when building query for fact.`);

            }

            return operators[operator].query(property, value, 'db');

        } catch (error) {

            throw this.error.handle({ error, caller: '_queryFactDB' });

        }

    }

    /**
     * Process rules against provide data
     * @param {object=} options - available options
     * @param {object=} [options.rules={}] - rules object
     * @returns {Promise<string>} returns sql where statement
     */
    async _queryRuleDB ({ rules = {} } = {}) {

        this.debug('_queryRuleDB');
        try {

            let subStatement = '';
            switch (rules.type) {

                case 'all':

                    for (let i = 0; i < rules.children.length; i++) {

                        if (subStatement !== '') {

                            subStatement += ' AND ';

                        }
                        subStatement += await this._queryRuleDB({ rules: rules.children[i] });

                    }

                    if (rules.children.length > 1) {

                        subStatement = `(${subStatement})`;

                    }

                    break;

                case 'any':

                    for (let i = 0; i < rules.children.length; i++) {

                        if (subStatement !== '') {

                            subStatement += ' OR ';

                        }
                        subStatement += await this._queryRuleDB({ rules: rules.children[i] });

                    }

                    if (rules.children.length > 1) {

                        subStatement = `(${subStatement})`;

                    }

                    break;

                case 'fact':

                    subStatement = await this._queryFactDB({ property: rules.property, operator: rules.operator, value: rules.value });
                    break;

                default:

                    throw new Error(`Unrecongized rule type '${rules.type}'.`);

            }

            return subStatement;

        } catch (error) {

            throw this.error.handle({ error, caller: '_queryRuleDB' });

        }

    }

}

module.exports = Rules;
