var _ = require('underscore');

var Deferred = module.exports = function() {
    this._fired = false;
    this._firing = false;
    this._successCbs = [];
    this._failureCbs = [];
    this._args = [];
    this._success = undefined; // undefined - not done yet, true = success, false = failure
};

Deferred.prototype.resolve = function() {
    return this._fireCallbacks(this._successCbs, true, arguments);
};

Deferred.prototype.reject = function() {
    return this._fireCallbacks(this._failureCbs, false, arguments);
};

Deferred.prototype._fireCallbacks = function(cbs, success, args) {
    if ( !(this._firing || this._fired) ) {
        try {
            // _args and _success must be set before _firing
            this._args = args;
            this._success = success;
            this._firing = true;

            while ( cbs[0] ) {
                cbs.shift().apply(this, this._args);
            }
        } finally {
            this._fired = true;
            this._firing = false;
        }
    }

    return this;
};

Deferred.prototype.success = function(cb) {
    if ( !(this._firing || this._fired) ) {
        this._successCbs.push(cb);
    } else if ( this._success === true ) {
        // already firing or fired
        while ( this._firing ) { /* wait for firing to end */ }
        cb.apply(this, this._args);
    }

    return this;
};

Deferred.prototype.fail = function(cb) {
    if ( !(this._firing || this._fired) ) {
        this._failureCbs.push(cb);
    } else if ( this._success === false ) {
        // already firing or fired
        while ( this._firing ) { /* wait for firing to end */ }
        cb.apply(this, this._args);
    }

    return this;
};

Deferred.prototype.promise = function() {
    var self = this;

    return {
        success: function(cb) { return self.success(cb); },
        fail: function(cb) { return self.fail(cb); }
    };
};
