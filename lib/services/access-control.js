'use strict';

const debug = require('debug')('layerize:access-control');
const { errors } = require('../utils');

/**
 * The AccessControl class handles permission verification.
 */
class AccessControl {

    /**
     * Create an AccessControl instance.
     * @param {object} config - available config
     * @param {array} config.grants - an array of grants
     */
    constructor ({ grants = {} } = {}) {

        this.grants = grants;
        this.debug = debug;
        this.error = errors;

    }

    /**
     * Get access control object for provided role, if role is an array then it must be true for all named roles
     * @param {object=} options - available options
     * @param {array} [options.role=[]] - role
     * @param {string|array} [options.action=''] - action
     * @param {string|array} [options.resource=''] - resource
     * @param {object} options.context - data object the permission is analyzed against
     * @returns {Promise<permission>} returns access control grant object
     */
    async can ({ role = [], action = '', resource = '', context } = {}) {

        this.debug('can');

        try {

            if (role.length === 0) {

                throw new Error('You must provided at least one role.');

            }

            let aryGrants = [];

            for (let i = 0; i < role.length; i++) {

                let rol = role[i];

                if (typeof this.grants[rol] === 'undefined') {

                    throw new Error(`The provided role '${rol}' could not be found in initialized grants.`);

                }

                let objRole = this.grants[rol];

                // filter grants down to the requested resource and action
                let grants = objRole.grants.filter(grant => (grant.resource === resource && grant.action === action));

                let objSubGrant = {
                    granted: false,
                    grant: {
                        pliminary: false,
                        full: false
                    },
                    _: null
                };

                if (grants.length > 0) {

                    let permission = grants[0];

                    // add role for visibilty
                    permission.role = rol;

                    let pliminary_grant = true;
                    let full_grant = false;

                    if (typeof permission.condition !== 'undefined' && typeof permission.condition === 'object' && Object.keys(permission.condition).length > 0) {

                        if (typeof context !== 'undefined' && typeof context === 'object') {

                            // TODO: send to conditionals parser which returns a boolean
                            full_grant = true;

                            // if conditionals fail then set
                            // full_grant = false;
                            // pliminary_grant = false;

                        } else {

                            // ignore because no context passed.. this is considered a pliminary check
                            pliminary_grant = true;

                        }

                    } else {

                        full_grant = true;

                    }

                    objSubGrant = {
                        granted: (pliminary_grant || full_grant),
                        grant: {
                            pliminary: pliminary_grant,
                            full: full_grant
                        },
                        _: permission
                    };

                }

                aryGrants.push(objSubGrant);

                if (objSubGrant.granted === false) {

                    break;

                }

            }

            let objGrant = {
                resource,
                action,
                granted: true,
                grant: {
                    pliminary: true,
                    full: true
                },
                _: aryGrants.map(grant => grant._)
            };

            for (let x = 0; x < aryGrants.length; x++) {

                let grant = aryGrants[x];

                if (grant.granted === false || grant.grant.pliminary === false) {

                    objGrant.granted = false;
                    objGrant.grant.pliminary = false;
                    objGrant.grant.full = false;

                    break;

                } else if (grant.grant.full === false) {

                    objGrant.grant.full = false;

                }

            }

            return objGrant;

        } catch (error) {

            throw this.error.handle({ error, caller: 'can' });

        }

    }

    /**
     * Get access control list for provided role, if role is an array then it must be true for all named roles
     * @param {object=} options - available options
     * @param {array=} [options.role=[]] - role
     * @param {array=} options.grants - grant
     * @returns {Promise<permission>} returns access control grant object
     */
    async accessList ({ role = [], grants = {} } = {}) {

        this.debug('accessList');

        try {

            if (role.length === 0) {

                throw new Error('You must provided at least one role.');

            }

            if (Object.keys(grants).length === 0) {

                throw new Error('You must provided a grants object.');

            }

            let lngRoles = role.length;

            // grab last role and populate into an object, so we can begin removing any permissions the parent does not have
            let grantKeys = grants[role[lngRoles - 1]].grants;
            let objGrants = Object.assign({}, ...grantKeys.map(item => ({ [item]: true })));

            // loop through the remaining roles to remove any permissions that the child does not have access to, as the parent must have access for the child to.
            for (let i = lngRoles - 1; i > 0; i--) {

                let rol = role[i];

                if (typeof grants[rol] === 'undefined') {

                    throw new Error(`The provided role '${rol}' could not be found in passed grants.`);

                }

                let permissions = grants[rol];
                let roleGrants = Object.assign({}, ...permissions.map(item => ({ [item]: true })));

                // loop through the original role and compare to parent, if not found then remove from the child
                for (let x = 0; x < grantKeys.length; x++) {

                    let grantKey = grantKeys[x];
                    if (roleGrants[grantKey] !== true) {

                        delete objGrants[grantKey];

                    }

                }

            }

            return objGrants;

        } catch (error) {

            throw this.error.handle({ error, caller: 'accessList' });

        }

    }

}

module.exports = AccessControl;
