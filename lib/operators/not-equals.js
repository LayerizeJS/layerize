'use strict';

module.exports = {

    parse: (fact, value) => {

        if (typeof fact === 'undefined') {

            return false;

        }

        return fact !== value;

    },

    query: (property, value, type) => {

        let statement;
        switch (type) {

            case 'db':

                statement = `${property} != '${value}'`;
                break;

            case 'es':
                statement = null;
                break;

        }

        return statement;

    }

};

