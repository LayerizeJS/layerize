'use strict';

let Layerize = require('../lib/index.js');

describe('layerize', () => {

    describe('layerize()', () => {

        it('should initiate', async () => {

            let layerize = new Layerize({ schemas: './test/data/schemas/**/*.json' });
            await layerize.initiate();

        });

    });

});
