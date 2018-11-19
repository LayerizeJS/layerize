'use strict';
/* eslint-disable max-len, new-cap, camelcase*/

const Redis = require('ioredis');
const RedisSMQ = require('rsmq');
const errors = require('./errors');
const debug = require('debug')('enspire:utils:cache');
const { promisify } = require('util');
const redislock = require('redis-lock');
const eventEmitter = require('redis-eventemitter');
let redis;
let error;
let lock;
let rsmq;
let redisEvents;

module.exports = {
    init: (port = '', url = '') => {

        redis = new Redis(port, url);
        error = errors;
        lock = promisify(redislock(redis));
        rsmq = new RedisSMQ({ host: url, port });
        redisEvents = eventEmitter({ prefix: '', url: `redis://${url}:${port}` });
        redisEvents.on('error', (err) => debug('Error on eventEmitter:', err));

    },
    setLock: async (...arg) => {

        return lock(...arg);

    },
    cache: {
        key: (...args) => {

            let aryArgs = Array.prototype.slice.call(args);
            return aryArgs.join(':').toUpperCase();

        },
        get: async key => {

            try {

                let checkIfMap = response => {

                    if (/^\{[\S\s]*}$/.test(response)) {

                        try {

                            response = JSON.parse(response);

                        } catch (e) {
                        }

                    }
                    return response;

                };

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

            return redis.del(...args);

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
        mset: (...args) => {

            let aryArgs = Array.prototype.slice.call(args);

            aryArgs.forEach((obj, i) => {

                if (typeof obj === 'object' && obj !== null) {

                    aryArgs[i] = JSON.stringify(obj);

                } else {

                    aryArgs[i] = String(obj);

                }

            });

            return redis.mset(...aryArgs);

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
            get: (key, field) => {

                if (typeof field === 'undefined') {

                    return redis.hgetall(key);

                } else {

                    return redis.hget(key, field);

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
    events: {
        emit: async (...arg) => { // channel, messages...

            debug('emit', ...arg);
            return await redisEvents.emit(...arg);

        },
        on: async (...arg) => { // pattern, (channel, messages...) => { ... }

            return redisEvents.on(...arg);

        },
        removeListener: async (...arg) => { // pattern, listener

            return redisEvents.removeListener(...arg);

        },
        removeAllListeners: async (...arg) => { // pattern

            return redisEvents.removeAllListeners(...arg);

        }
    },
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
