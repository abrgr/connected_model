var utils = module.exports = {};

utils.isString = function(s) { return 'string' === typeof(s) || s instanceof String; };

utils.isArray = function(obj) { return Object.prototype.toString.call(obj) === "[object Array]"; }
