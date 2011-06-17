var express = require('express')
    , HTTPSServer = express.HTTPSServer
    , HTTPServer = express.HTTPServer;

module.exports.connectModel 
    = HTTPServer.prototype.connectModel 
    = HTTPSServer.prototype.connectModel = function(baseRoute, model, modelName, options) {
    var app = this;

    if ( !modelName ) {
        modelName = baseRoute.substring(baseRoute.lastIndexOf('/') + 1);
    }

    options = options || {};

    // our template is haml
    // TODO: should we namespace this so we don't effect the actual creator of app?
    app.register('.haml', require('hamljs'));

    var routes = {};

    var connectedModelStaticFunctions = getConnectedModelFunctions(model, false);
    connectedModelStaticFunctions.forEach(function(functionName) {
        // add get routes for static functions
        var route = baseRoute + '/' + functionName;
        app.get(route + '(/?)*', function(req, res) {
            params = getParamsFromUrl(req.url, route);

            res.send(model[functionName].apply(null, params), {'content-type': 'application/json'});
        });

        routes[functionName] = route;
    });

    var connectedModelInstanceFunctions = getConnectedModelFunctions(model, true);
    connectedModelInstanceFunctions.forEach(function(functionName) { 
        // add post routes for instance functions that expect model(s) posted to them
        var route = baseRoute + '/' + functionName;
        app.post(route, function(req, res) {
            params = getParamsFromUrl(req.url, route);
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
                res.send(model.prototype[functionName].apply(target, params), {'content-type': 'application/json'});
            });
        });

        routes[functionName] = route;
    });

    app.get(baseRoute + '/model.js', function(req, res) {
        res.render('connected_model.haml', 
			{
             layout: false, 
             routes: routes,
             post: getClientSidePost(),	
             get: getClientSideGet(),	
             connected_instance_functions: connectedModelInstanceFunctions, 
             connected_static_functions: connectedModelStaticFunctions, 
             main_obj: renderClassDefinition(model, modelName, connectedModelInstanceFunctions.concat(connectedModelStaticFunctions)), 
             main_obj_name: modelName
            }
        );
    });
}

function getConnectedModelFunctions(model, getInstanceFunctions) {
    // figure out which methods we should expose to the front end
    if ( model instanceof Function ) {
        // model is a constructor
        model = getInstanceFunctions ? model.prototype : model;

        return Object.keys(model).map(function(modelPropertyName) {
            var modelProperty = model[modelPropertyName];
            if ( modelProperty instanceof Function && modelProperty.expose ) {
                return modelPropertyName;
            }
        }).filter(function(modelPropertyName) { return !!modelPropertyName; });
    }

    throw Error('connected_model only handles functions as models');
}

function getParamsFromUrl(url, route) {
    var paramsStr = url.substring(route.length);
    if ( paramsStr[0] === '/' ) {
        paramsStr = paramsStr.substring(1);
    }
    return paramsStr.split('/') || [];
}

/**
* Render `ctor` with the given `name`, skipping any properties in `propertiesToSkip`.
*
* @param {Function} ctor
* @param {String} name
* @param {Array} propertiesToSkip
* @return {String}
* @api private
*/
function renderClassDefinition(ctor, name, propertiesToSkip) {
    propertiesToSkip = propertiesToSkip || [];

    return ['var ' + name + ' = ' + string(ctor.prototype.constructor) + ';'].concat( 
        Object.keys(ctor.prototype).filter(function(key) {
            return !~propertiesToSkip.indexOf(key);
        }).map(function(key){
            var val = ctor.prototype[key];
            return name + '.prototype["' + key + '"] = ' + string(val) + ';';
        }).concat(
            Object.keys(ctor).filter(function(key) {
                return !~propertiesToSkip.indexOf(key);
            }).map(function(key) {
                var val = ctor[key];
                return name + '["' + key + '"] = ' + string(val) + ';';
            })
        )
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

function getClientSidePost() {
    return function(url) {
        return '\nvar urlArgs = [""].concat(\nArray.prototype.map.call(\nArray.prototype.slice.call(arguments, 1), \nfunction(arg){\nreturn encodeURI(arg);}));\nurlArgs=urlArgs.join("/");\nreturn $.post(\'' + url + '\' + urlArgs, this);'; 
    }
}

function getClientSideGet() {
    return function(url) {
        return '\nvar urlArgs = [""].concat(Array.prototype.map.call(arguments, \nfunction(arg){\nreturn encodeURI(arg);}));\nurlArgs=urlArgs.join("/");\n\nreturn $.get("' + url + '" + urlArgs);'; 
    }
}
