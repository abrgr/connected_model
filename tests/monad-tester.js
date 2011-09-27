var _ = require('underscore');

function binder(methodName) {
    this.currentArgs[methodName] = Array.prototype.slice.call(arguments, 1);

    return this;
}

/**
 * MonadTester is meant to be used to test monads.
 *
 * Example:
 *   var tester = new MonadTester('select', 'where', 'from').select(['abc', 'def']).from('tableName').where('w=? AND v=?', [12, 34])
 *                                                          .EXPECT('execute').andReturn(Deferred.resolved([{abc: 'abc', def: 'def'}]);
 *
 * @onstructor
 * @param {Array|String...} methodNames - Either a single array of method names or variadic string method name arguments.
 */
function MonadTester() {
    if ( arguments.length < 1 ) {
        throw new Error("You must provide method names to the MonadTester constructor");
    }

    var args = Array.prototype.slice.call(arguments);
    if ( arguments.length === 1 && _.isArray(arguments[0]) ) {
        args = Array.prototype.slice.call(arguments[0]);
    }

    this.currentArgs = {};
    this.retMethodsByArgs = [];

    var self = this;
    args.forEach(function(methodName) {
        if ( !_.isString(methodName) ) {
            throw new Error("Method names must be strings");
        }

        self[methodName] = binder.bind(self, methodName);
    });
}



MonadTester.prototype.EXPECT = function(retMethod) {
    var self = this;
    if ( !this[retMethod] ) {
        this[retMethod] = function() {
            var selected = self.retMethodsByArgs.filter(function(argsAndRet) {
                return _.isEqual(argsAndRet.args, self.currentArgs);
            });

            if ( selected.length !== 1 ) {
                console.error('Found', selected.length, 'currentArgs', self.currentArgs, 'retMethodsByArgs', self.retMethodsByArgs);

                throw new Error("Multiple or no returns possible");
            }

            selected = selected[0];

            self.currentArgs = {};
            if ( !!selected.useCb ) {
                // callback
                return arguments[selected.cbIdx].apply(undefined, selected.cbArgs);
            }

            return selected.ret;
        };
    }

    return {
        andReturn: function(retVal) {
            self.retMethodsByArgs.push({args: self.currentArgs, ret: retVal});
            self.currentArgs = {};

            return self;
        },
        andCall: function(cbIdx) {
            return {
                with: function() {
                    self.retMethodsByArgs.push({args: self.currentArgs, cbArgs: Array.prototype.slice.call(arguments), cbIdx: cbIdx, useCb: true});
                    self.currentArgs = {};

                    return self;
                }
            };
        }
    };
};

module.exports = MonadTester;
