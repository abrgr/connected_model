var _ = require('underscore');

var utils = module.exports = {};

utils.classify = function(obj, Type) {
    return new Type(obj);
};

utils.objectZip = function(array1, array2) {
    if ( !(_.isArray(array1) && _.isArray(array2) && array1.length === array2.length) ) {
        throw new Error('objectZip requires 2 arrays of equal length');
    }

    var ret = {};
    var i=0, len=array1.length;
    for ( ; i<len; ++i ) {
        ret[array1[i]] = array2[i];
    }

    return ret;
};
