var _ = require('underscore');

var utils = module.exports = {};

utils.classify = function(obj, Type) {
    var ctor = function(){};
    ctor.prototype = Type.prototype;
    var classifiedObj = new ctor();
    for ( var key in obj ) {
        if ( obj.hasOwnProperty(key) ) {
            classifiedObj[key] = obj[key];
        }
    }

    return classifiedObj;
}

utils.objectZip = function(array1, array2) {
    if ( !(_.isArray(array1) && _.isArray(array2) && array1.length === array2.length) ) {
        throw new Error('objectZip requires 2 arrays of equal length');
    }

    var ret = {};
    for ( var i=0, len=array1.length; i<len; ++i ) {
        ret[array1[i]] = array2[i];
    }

    return ret;
}
