'use strict';
/* eslint-disable max-len, new-cap, camelcase*/

const Redis = require('ioredis');
const RedisSMQ = require('rsmq');
const errors = require('./errors');
const debug = require('debug')('layerize:utils:cache');
const { promisify } = require('util');
const redislock = require('redis-lock');
const NodeCache = require('node-cache');

const inMemoryCacheTTL = 60;
const inMemoryCache = new NodeCache({ stdTTL: inMemoryCacheTTL });

// The following module throws errors when socketIORedis is used, so it is being depricated
// const eventEmitter = require('redis-eventemitter');

let redis;
let error;
let lock;
let rsmq;
// let redisEvents;

/**
 * private internal function for _esFilter
 * @access private
 * @param {string} response - string from redis
 * @returns {object} es body statement
 */
let checkIfMap = response => {

    if (/^\{[\S\s]*}$/.test(response)) {

        try {

            response = JSON.parse(response);

        } catch (e) {
        }

    } else if (response === 'null') {

        response = null;

    } else if (response === 'true') {

        response = true;

    } else if (response === 'false') {

        response = false;

    }
    return response;

};

const redisUtil = {
    init: (port = '', host = '') => {

        try {

            redis = new Redis(port, host);
            // redis = new Redis.Cluster([
            //     {
            //         port,
            //         host
            //     },
            //     {
            //         port: 7001,
            //         host
            //     },
            //     {
            //         port: 7002,
            //         host
            //     },
            //     {
            //         port: 7003,
            //         host
            //     },
            //     {
            //         port: 7004,
            //         host
            //     },
            //     {
            //         port: 7005,
            //         host
            //     }
            // ]);
            error = errors;
            lock = promisify(redislock(redis));
            rsmq = new RedisSMQ({ client: redis });

            // redisEvents = eventEmitter({
            //     prefix: '',
            //     url: 'tempfix-as-module-throws-error-when-blank--this-url-is-not-used-because-pub-sub-is-being-set',
            //     pub: redis,
            //     sub: redis
            // });
            // redisEvents.on('error', (err) => debug('Error on eventEmitter:', err));

            return redis;

        } catch (e) {

            // eslint-disable-next-line no-console
            console.error(e);
            throw e;

        }

    },
    setLock: (...arg) => {

        return lock(...arg);

    },
    cache: {
        inMemory: {
            get: async (key, { ttl = inMemoryCacheTTL, onEmptyCache = null } = {}) => {

                try {

                    let data = inMemoryCache.get(key);

                    if (typeof data === 'undefined') {

                        if (onEmptyCache !== null) {

                            data = await onEmptyCache(key);

                        } else {

                            data = await redisUtil.cache.get(key);

                        }

                        // store data in memory, even if null
                        inMemoryCache.set(key, data, ttl);

                        debug(`${key} was not found in-memory and was set for ${ttl} seconds`);

                    } else {

                        debug(`${key} was found in-memory`);

                    }

                    return data;

                } catch (err) {

                    throw error.handle({ error: err, caller: 'get' });

                }

            }
        },
        key: (...args) => {

            let aryArgs = Array.prototype.slice.call(args);

            if (aryArgs.indexOf(undefined) > -1) {

                throw new Error('cache.key should not contain an \'undefined\', as it is probably from an error in your code.');

            }

            return aryArgs.join(':').toUpperCase();

        },
        get: async key => {

            try {

                if (Array.isArray(key)) {

                    if (key.length === 0) {

                        throw new Error('redis.get: key length is zero');

                    }

                    let aryResponse = await redis.mget(...key);
                    let aryNew = [];
                    aryResponse.forEach(response => {

                        aryNew.push(checkIfMap(response));

                    });
                    return aryNew;

                } else {

                    let response = await redis.get(key);
                    return checkIfMap(response);

                }

            } catch (err) {

                throw error.handle({ error: err, caller: 'get' });

            }

        },
        getByPattern: key => { //key example "prefix*"

            return new Promise((resolve, reject) => {

                // let keys = [];

                // let nodes = redis.nodes('master');

                // /**
                //  * private internal function
                //  * @access private
                //  * @param {num} nodeIndex - master node array index from redis
                //  * @returns {undefined}
                // */
                // let scanNode = (nodeIndex) => {

                //     let node = nodes[nodeIndex];
                //     let stream = node.scanStream({
                //         // only returns keys following the pattern of "key"
                //         match: key,
                //         // returns approximately 100 elements per call
                //         count: 1000
                //     });

                //     stream.on('data', resultKeys => {

                //         // `resultKeys` is an array of strings representing key names
                //         for (let i = 0; i < resultKeys.length; i++) {

                //             keys.push(resultKeys[i]);

                //         }

                //     });

                //     stream.on('end', () => {

                //         if (++nodeIndex < nodes.length) {

                //             scanNode(nodeIndex);

                //         } else {

                //             resolve(keys);

                //         }

                //     });

                //     stream.on('error', reject);

                // };
                // scanNode(0);

                let stream = redis.scanStream({
                    // only returns keys following the pattern of "key"
                    match: key,
                    // returns approximately 100 elements per call
                    count: 100
                });

                let keys = [];

                stream.on('data', resultKeys => {

                    // `resultKeys` is an array of strings representing key names
                    for (let i = 0; i < resultKeys.length; i++) {

                        keys.push(resultKeys[i]);

                    }

                });

                stream.on('end', () => {

                    resolve(keys);

                });

                stream.on('error', reject);

            });

        },
        deleteByPattern: key => { //key example "prefix*"

            return new Promise((resolve, reject) => {

                // let nodes = redis.nodes('master');

                // /**
                //  * private internal function
                //  * @access private
                //  * @param {num} nodeIndex - master node array index from redis
                //  * @returns {undefined}
                // */
                // let scanNode = (nodeIndex) => {

                //     let node = nodes[nodeIndex];
                //     let stream = node.scanStream({
                //     // only returns keys following the pattern of "key"
                //         match: key,
                //         // returns approximately 100 elements per call
                //         count: 10000
                //     });

                //     let keys = [];
                //     stream.on('data', resultKeys => {

                //         // `resultKeys` is an array of strings representing key names
                //         for (let i = 0; i < resultKeys.length; i++) {

                //             keys.push(resultKeys[i]);

                //         }

                //     });

                //     stream.on('end', async () => {

                //         if (keys.length > 0) {

                //             await node.unlink(keys);

                //         }

                //         if (++nodeIndex < nodes.length) {

                //             scanNode(nodeIndex);

                //         } else {

                //             resolve({ success: true });

                //         }

                //     });

                //     stream.on('error', reject);

                // };
                // scanNode(0);

                let stream = redis.scanStream({
                // only returns keys following the pattern of "key"
                    match: key,
                    // returns approximately 100 elements per call
                    count: 100
                });

                let keys = [];
                stream.on('data', resultKeys => {

                    // `resultKeys` is an array of strings representing key names
                    for (let i = 0; i < resultKeys.length; i++) {

                        keys.push(resultKeys[i]);

                    }

                });

                stream.on('end', () => {

                    if (keys.length > 0) {

                        redis.unlink(keys).then(resolve, reject);

                    } else {

                        resolve();

                    }

                });

                stream.on('error', reject);

            });

        },
        multi: (calls = []) => {

            return new Promise((resolve, reject) => {

                let multi = redis.multi();
                calls.forEach(call => {

                    multi = multi[call.command.toLowerCase()](...call.args);

                });

                multi.exec((err, results) => {

                    // results are returned [ [ err, result ], [ err, result ] ] from redis

                    if (err) {

                        reject(err);

                    } else {

                        debug(results);
                        resolve(results);

                    }

                });

            });

        },
        clear: (...args) => {

            // remove duplicates
            args = [ ...new Set(args) ];

            return redis.del(...args);

        },
        exists: (...args) => {

            // remove duplicates
            args = [ ...new Set(args) ];

            return redis.exists(...args);

        },
        set: (key, exp, obj) => {

            let str;
            if (typeof obj === 'object' && obj !== null) {

                str = JSON.stringify(obj);

            } else {

                str = String(obj);

            }
            return redis.setex(key, exp, str);

        },
        mset: (keys = [], exp = '') => {

            return new Promise((resolve, reject) => {

                let multi = redis.multi();

                for (let i = 0; i < keys.length; i++) {

                    let obj = keys[i];
                    if (typeof obj.value === 'object' && obj.value !== null) {

                        obj.value = JSON.stringify(obj.value);

                    } else {

                        obj.value = String(obj.value);

                    }

                    if (exp !== '') {

                        multi = multi.setex(obj.key, exp, obj.value);

                    } else {

                        multi = multi.set(obj.key, obj.value);

                    }

                }

                if (keys.length > 0) {

                    multi.exec((err, results) => {

                        // results are returned [ [ err, result ], [ err, result ] ] from redis

                        if (err) {

                            reject(err);

                        } else {

                            debug(results);
                            resolve(results);

                        }

                    });

                } else {

                    resolve([]);

                }

            });

        },
        hash: {
            clear: (...args) => {

                return redis.hdel(...args);

            },
            set: async (key, obj, bolReplaceAll = false) => {

                let args = [ key ];
                let fields = Object.keys(obj);
                for (let i = 0; i < fields.length; i++) {

                    let field = fields[i];
                    let value = obj[field];

                    if (typeof value === 'object' && value !== null) {

                        value = JSON.stringify(value);

                    }

                    if (bolReplaceAll === false) {

                        await redis.hset(key, field, value);

                    } else {

                        args.push(field);
                        args.push(value);

                    }

                }

                if (bolReplaceAll === true) {

                    return redis.hmset(...args);

                } else {

                    return { success: true };

                }

            },
            exists: (key, field) => {

                return redis.hexists(key, field);

            },
            keys: (key) => {

                return redis.hkeys(key);

            },
            len: (key) => {

                return redis.hlen(key);

            },
            incrby: (...args) => {

                return redis.hincrbyfloat(...args);

            },
            get: async (key, field) => {

                if (typeof field === 'undefined' || (Array.isArray(field) && field.length === 0)) {

                    let results = await redis.hgetall(key);

                    let keys = Object.keys(results);
                    for (let i = 0; i < keys.length; i++) {

                        results[keys[i]] = checkIfMap(results[keys[i]]);

                    }

                    return results;

                } else if (Array.isArray(field) && field.length > 0) {

                    let args = [ key ].concat(field);
                    let results = await redis.hmget(...args);

                    for (let i = 0; i < results.length; i++) {

                        results[i] = checkIfMap(results[i]);

                    }

                    return results;

                } else {

                    return checkIfMap(await redis.hget(key, field));

                }

            }
        },
        list: {
            add: (key, val, score) => {

                if (typeof score === 'undefined') {

                    score = new Date().getTime();

                }
                return redis.zadd(key, score, val);

            },
            remove: (key, val) => {

                return redis.zrem(key, val);

            },
            range: (key, numStart, numEnd, bolReversed) => {

                if (bolReversed) {

                    return redis.zrevrange(key, numStart, numEnd);

                } else {

                    return redis.zrange(key, numStart, numEnd);

                }

            },
            count: (key) => {

                return redis.zcard(key);

            },
            get: (key, val) => {

                return redis.zscore(key, val);

            },
            pluckAll: (key) => {

                return new Promise((resolve, reject) => {

                    redis.multi().zrange(key, 0, -1).zremrangebyrank(key, 0, -1).exec((err, results) => {

                        if (err) {

                            reject(err);

                        } else {

                            debug(results);
                            resolve(results[0][1]);

                        }

                    });

                });

            },
            all: (key, bolReversed) => {

                if (bolReversed) {

                    return redis.zrevrange(key, 0, -1);

                } else {

                    return redis.zrange(key, 0, -1);

                }

            }
        }
    },
    // events: {
    //     emit: async (...arg) => { // channel, messages...

    //         debug('emit', ...arg);
    //         return await redisEvents.emit(...arg);

    //     },
    //     on: async (...arg) => { // pattern, (channel, messages...) => { ... }

    //         return redisEvents.on(...arg);

    //     },
    //     removeListener: async (...arg) => { // pattern, listener

    //         return redisEvents.removeListener(...arg);

    //     },
    //     removeAllListeners: async (...arg) => { // pattern

    //         return redisEvents.removeAllListeners(...arg);

    //     }
    // },
    queue: {
        list: (...arg) => promisify(rsmq.listQueues)(...arg),

        create: (...arg) => promisify(rsmq.createQueue)(...arg),

        setAttributes: (...arg) => promisify(rsmq.setQueueAttributes)(...arg),

        getAttributes: (...arg) => promisify(rsmq.getQueueAttributes)(...arg),

        delete: (...arg) => promisify(rsmq.deleteQueue)(...arg),

        message: {
            send: (...arg) => promisify(rsmq.sendMessage)(...arg),

            receive: (...arg) => promisify(rsmq.receiveMessage)(...arg),

            delete: (...arg) => promisify(rsmq.deleteMessage)(...arg),

            pop: (...arg) => promisify(rsmq.popMessage)(...arg)
        }
    }

};

module.exports = redisUtil;
