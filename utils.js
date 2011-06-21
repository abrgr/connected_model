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
