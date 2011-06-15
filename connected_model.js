var express = require('express')
    , HTTPSServer = express.HTTPSServer
    , HTTPServer = express.HTTPServer;

module.exports.connect_model 
    = HTTPServer.prototype.connect_model 
    = HTTPSServer.prototype.connect_model = function(baseRoute, model, model_name) {
    var app = this;

    if ( !model_name ) {
        model_name = baseRoute.substring(baseRoute.lastIndexOf('/') + 1);
    }

    // our template is haml
    // TODO: should we namespace this so we don't effect the actual creator of app?
    app.register('.haml', require('hamljs'));

    var connected_model_functions = get_connected_model_functions(model);
    connected_model_functions.forEach(function(function_name) {
        // add routes for function
        var route = baseRoute + '/' + function_name;
        app.get(route + '/?*', function(req, res) {
            params = get_params_from_url(req.url, route);

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
    });

    app.get(baseRoute + '/model.js', function(req, res) {
        res.render('connected_model.haml', {layout: false, connected_functions: connected_model_functions, main_obj: render_class_definition(model.prototype, model_name, connected_model_functions), main_obj_name: model_name});
    });
}

var get_connected_model_functions = function(model) {
    // figure out which methods we should expose to the front end
    if ( model instanceof Function ) {
        // model is a constructor
        return Object.keys(model.prototype).map(function(model_property_name) {
            var model_property = model.prototype[model_property_name];
            if ( model_property instanceof Function && model_property.expose ) {
                return model_property_name;
            }
        }).filter(function(model_property_name) { return !!model_property_name; });
    }

    throw Error('connected_model only handles functions as models');
}

var get_params_from_url = function(url, route) {
    var params_str = url.substring(route.length);
    if ( params_str[0] === '/' ) {
        params_str = params_str.substring(1);
    }
    return params_str.split('/') || [];
}

/**
* Render `proto` with the given `name`, skipping any properties in `properties_to_skip`.
*
* @param {Object} obj
* @param {String} name
* @param {Array} properties_to_skip
* @return {String}
* @api private
*/
function render_class_definition(proto, name, properties_to_skip) {
    properties_to_skip = properties_to_skip || [];

    return ['var ' + name + ' = ' + string(proto.constructor) + ';'].concat( 
        Object.keys(proto).filter(function(key) {
            return !~properties_to_skip.indexOf(key);
        }).map(function(key){
            var val = proto[key];
            return name + '["' + key + '"] = ' + string(val) + ';';
        })
    ).join('\n');
}

/**
* Return a string representation of `obj`.
*
* @param {Mixed} obj
* @return {String}
* @api private
*/
function string(obj) {
  if ('function' == typeof obj) {
    return obj.toString();
  } else if (obj instanceof Date) {
    return 'new Date("' + obj + '")';
  } else if (Array.isArray(obj)) {
    return '[' + obj.map(string).join(', ') + ']';
  } else if ('[object Object]' == Object.prototype.toString.call(obj)) {
    return '{' + Object.keys(obj).map(function(key){
      return '"' + key + '":' + string(obj[key]);
    }).join(', ') + '}';
  } else {
    return JSON.stringify(obj);
  }
}
