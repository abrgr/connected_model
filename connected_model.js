var express = require('express')
    , HTTPSServer = express.HTTPSServer
    , HTTPServer = express.HTTPServer;

module.exports.connect_model 
    = HTTPServer.prototype.connect_model 
    = HTTPSServer.prototype.connect_model = function(baseRoute, model) {
    var app = this;
    var connected_model_functions = get_connected_model_functions(model);

    connected_model_functions.forEach(function(function_name) {
        // add routes for function
        var route = baseRoute + '/' + function_name;
        app.get(route + '/?*', function(req, res) {
            params = get_params_from_url(req.url);

            res.send(model.prototype[function_name].apply(null, params), {'content-type': 'application/json'});
        });

        app.post(route + '/?*', function(req, res) {
            params = get_params_from_url(req.url);
            var targets = null;
            if ( req.body instanceof Array ) {
                targets = req.body;
            } else if ( req.body instanceof Object ) {
                targets = [req.body];
            }

            if ( !!targets ) {
                throw Error('Invalid post body');
            }

            targets.forEach(function(target) {
                target.prototype = model.prototype;
                res.send(model.prototype[function_name].apply(target, params), {'content-type': 'application/json'});
            });
    });
}

var get_connected_model_functions = function(model) {
    // figure out which methods we should expose to the front end
    var connected_model_functions = [];
    if ( model instanceof Function ) {
        // model is a constructor
        for ( var model_property_name in model.prototype ) {
            var model_property = model.prototype[model_property_name];
            if ( model_property instanceof Function && model_property.expose ) {
                connected_model_functions.push(model_property_name);
            }
        }
    }

    return connected_model_functions;
}

var get_params_from_url = function(url) {
    var params_str = req.url.substring(route.length);
    if ( params_str[0] === '/' ) {
        params_str = params_str.substring(1);
    }
    return params_str.split('/') || [];
}
