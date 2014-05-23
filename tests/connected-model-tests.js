var _ = require('underscore'),
    routes = require('routes'),
    fs = require('fs'),
    CM = require('..').connectedModel;

function TestModel(other) {
    this.a = other.a
    this.b = other.b
}

TestModel.prototype = {
    getA: function() { return a; }
};

module.exports.additionalIncludesTest = function(test) {
    var modelOptions = {routes: routes, jqueryInclude: 'jquery', exposeDynamicJsRoute: false};
    var model = CM('/scripts/models/test-model', TestModel, 'TestModel', _.extend({additionalIncludes: ['extra-include']}, modelOptions))

    model.renderSync('/tmp');

    console.log(fs.readFileSync('/tmp/scripts/models/test-model/model.js').toString('ascii'));
    test.done();
};
