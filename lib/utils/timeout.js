'use strict';

module.exports = {
    timeout: (ms) => {

        return new Promise(resolve => setTimeout(resolve, ms));

    }
};
