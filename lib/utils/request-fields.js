'use strict';

const Notation = require('notation');
const Rules = require('../services/rules');
const extend = require('extend');

module.exports = function (data = {}, filter) {

    let rules = new Rules();

    /**
     * _dataFilter
     * @param {object|array} data - data to be modified
     * @param {string|array} fields - string to be omitted or picked
     * @param {boolean} [bolStacked=true] - string to be omitted or picked
     * @returns {object|array} data
     */
    let _dataFilter = function (data, { objFields }) {

        if (objFields.fields.length === 1 && objFields.fields[0] === '*' && objFields.conditionals === false) {

            return data;

        } else {

            let fields = objFields.fields;
            if (objFields.conditionals === true) {

                fields = extend(false, [], fields);
                let keys = Object.keys(objFields.conditions);
                for (let i = 0; i < keys.length; i++) {

                    let property = keys[i];
                    let bol = rules._processRule({ rules: objFields.conditions[property], data });

                    if (bol === true) {

                        fields.push(`!${property}`);

                    }

                }

            }

            let notation = new Notation(data);
            return notation.filter(fields).value;

        }

    };

    /**
     * _prepFields
     * @param {string|array} fields - fields
     * @param {object} objFields - objFields
     * @returns {object} objFields
     */
    let _prepFields = (fields, objFields) => {

        if (!Array.isArray(fields)) {

            objFields.fields = fields.split(',');

        } else {

            objFields.fields = [];

            for (let i = 0; i < fields.length; i++) {

                let property = fields[i];

                if (typeof property === 'string') {

                    objFields.fields.push(property);

                } else {

                    if (typeof property.condition === 'object' && Object.keys(property.condition).length > 0 && property.property.charAt(0) === '!') {

                        objFields.conditionals = true;
                        objFields.conditions[property.property] = property.condition;

                    } else {

                        objFields.fields.push(`!${property.property}`);

                    }

                }

            }

        }

        return objFields;

    };

    /**
     * dataFilter
     * @param {object|array} data - data to be modified
     * @param {string|array} fields - string to be omitted or picked
     * @param {boolean} [bolStacked=true] - string to be omitted or picked
     * @returns {object|array} data
     */
    let dataFilter = (data, fields, bolStacked = true) => {

        let objFields = {
            conditionals: false,
            conditions: {},
            fields: []
        };

        if (bolStacked === true) {

            let aryStacked = [];
            for (let i = 0; i < fields.length; i++) {

                let objStackFields = {
                    conditionals: false,
                    conditions: {},
                    fields: []
                };

                let f = fields[i];

                objStackFields = _prepFields(f, objStackFields);

                objStackFields.fields = Notation.Glob.sort(objStackFields.fields);

                aryStacked.push(objStackFields);

            }

            objFields = aryStacked[0];

            // combine stack
            for (let x = 1; x < aryStacked.length; x++) {

                let objStack = aryStacked[x];

                if (objStack.fields[0] === '*') {

                    objFields.fields = Notation.Glob.normalize([...objFields.fields, ...objStack.fields]);

                } else {

                    // only grabbed remove fields to pass along so we can make sure they don't get added later
                    let removedFields = objFields.fields.filter(str => str.charAt(0) === '!');
                    if (removedFields.length > 0) {

                        objFields.fields = Notation.Glob.normalize([...removedFields, ...objStack.fields]);

                    } else {

                        objFields.fields = objStack.fields;

                    }

                }

                if (objStack.conditionals === true) {

                    objFields.conditionals = true;

                    let keys = Object.keys(objStack.conditions);
                    for (let y = 0; y < keys.length; y++) {

                        if (typeof objFields.conditions[keys] === 'undefined') {

                            objFields.conditions[keys] = objStack.conditions[keys];

                        } else {

                            // combine using an 'all' type rule
                            objFields.conditions[keys] = {
                                type: 'all',
                                children: [objFields.conditions[keys], objStack.conditions[keys]]
                            };

                        }

                    }

                }

            }

        } else {

            objFields = _prepFields(fields, objFields);

        }

        if (!Array.isArray(data)) {

            return _dataFilter(data, { objFields, bolStacked });

        } else {

            return data.map(o => _dataFilter(o, { objFields, bolStacked }));

        }

    };

    if (typeof filter === 'object' && !Array.isArray(filter) && Array.isArray(filter.stacked)) {

        return dataFilter(data, filter.stacked, true);

    } else {

        return dataFilter(data, filter, false);

    }

};
