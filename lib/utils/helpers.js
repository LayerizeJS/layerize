'use strict';
/* eslint-disable max-len */

const _ = require('underscore');

const helpers = {
    _,
    isEmail: (value) => {

        const pattern = /^$|^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
        return pattern.test(value);

    },
    isUndefinedEmptyNull: (value) => { // isUndefinedEmptyNull

        return _.isUndefined(value) || (_.isEmpty(value) && typeof value !== 'function') || _.isNull(value);

    },
    randomNumberXToY: (minVal, maxVal) => {

        var randVal = minVal + (Math.random() * (maxVal - minVal));
        return Math.round(randVal);

    },
    parseLiteral: (template, data, dataConst = 'data') => {

        // eslint-disable-next-line no-new-func
        return new Function('try { const ' + dataConst + ' = this; return `' + template + '`; } catch(e){ return template; }').call(data);

    },
    dateTime: () => (new Date()).toISOString()
};

module.exports = helpers;
