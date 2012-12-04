var express = require('express'),
    HTTPSServer = express.HTTPSServer,
    HTTPServer = express.HTTPServer,
    path = require('path'),
    fs = require('fs'),
    utils = require('./utils'),
    _ = require('underscore'),
    Deferred = require('deferred');

module.exports = function(baseRoute, model, modelName, options) {
    var app = this;

    if ( arguments.length === 1 && !_.isString(baseRoute) ) {
        // we just got an options object
        options = baseRoute; // options is the first and only param
        baseRoute = options.baseRoute;
        model = options.model;
        modelName = options.modelName;
    }

    if ( !modelName ) {
        modelName = baseRoute.substring(baseRoute.lastIndexOf('/') + 1);
    }

    options = options || {};

    var routes = {};
    var middlewareByRoute = {};

    var fixParams = function(req, fn, params) {
        if ( !!fn.sessionParams ) {
            Object.keys(fn.sessionParams).forEach(function(i) {
                params[i] = req.session[fn.sessionParams[i]];
            });
        }

        return params;
    };

    var connectedModelStaticFunctions = getConnectedModelFunctions(model, false);
    connectedModelStaticFunctions.forEach(function(functionName) {
        // add get routes for static functions
        var route = baseRoute + '/' + functionName;
        routes[functionName] = route;

        middlewareByRoute[route + '(/?).*'] = function(req, res, next) {
            var params = fixParams(req, model[functionName], getParamsFromUrl(req.url, route));

            var promise = model[functionName].apply(null, params);
            if ( _.isFunction(promise.success) && _.isFunction(promise.fail) ) {
                // model returned a promise as it should
                promise.success(function(result) {
                    res.send(result, {'content-type': getContentTypeForValue(result)});
                }).fail(function(err) {
                    return next(err);
                });
            } else {
                // we'll try to send whatever we got back
                res.send(result, {'content-type': getContentTypeForValue(result)});
            }
        };
    });

    var connectedModelInstanceFunctions = getConnectedModelFunctions(model, true);
    connectedModelInstanceFunctions.forEach(function(functionName) { 
        // add post routes for instance functions that expect model(s) posted to them
        var route = baseRoute + '/' + functionName;
        routes[functionName] = route;

        middlewareByRoute[route] = function(req, res, next) {
            var params = fixParams(req, model.prototype[functionName], getParamsFromUrl(req.url, route));

            var targets = null;
            var returnArray;

            if ( _.isArray(req.body) ) {
                targets = req.body;
                returnArray = true;
            } else if ( req.body instanceof Object ) {
                targets = [req.body];
                returnArray = false;
            }

            if ( !targets ) {
                return next(Error('Invalid post body for: ' + functionName));
            }

            Deferred.afterAll(targets.map(function(target) { 
                target = utils.classify(target, model);
                var promise = model.prototype[functionName].apply(target, params);
                if ( _.isFunction(promise.success) && _.isFunction(promise.fail) ) {
                    // model returned a promise as it should
                    return promise;
                } else {
                    // we'll try to send whatever we got back
                    return new Deferred(promise).promise();
                }
            })).success(function(result) {
                // result is an array of results from the above invocations, where each result is an array of args to success
                // we expect a single result for each invocation so we unzip the inner arrays
                result = result.map(function(res) { return res[0]; });
                if ( !returnArray ) {
                    // we converted the target into an array.  now just return our single result
                    result = result[0];
                }
                
                res.send(result, {'content-type': getContentTypeForValue(result)});
            }).fail(function(err) {
                next(err);
            });
        };
    });

    function renderJs(renderer, cb) {
        renderer.render(__dirname + '/../views/connected_model.jade', 
          {
             layout: false, 
             routes: routes,
             jqueryInclude: options.jqueryInclude,
             connected_instance_functions: connectedModelInstanceFunctions, 
             connected_static_functions: connectedModelStaticFunctions, 
             main_obj: renderClassDefinition(model, modelName, connectedModelInstanceFunctions.concat(connectedModelStaticFunctions)), 
             main_obj_name: modelName
          }, cb);
    }

    if ( options.exposeDynamicJsRoute ) {
        middlewareByRoute[baseRoute + '/model.js'] = function(req, res) {
            renderJs(res, function(a, script) { res.send(script, {'Content-Type': 'application/javascript'}); });
        };
    }

    if ( !!options.routes && _.isFunction(options.routes.addRoute) ) {
        Object.keys(routes).forEach(function(fnName) {
            options.routes.addRoute(modelName + '/' + fnName, routes[fnName]);
        });
    }

    return {
        middleware: function(req, res, next) {
            var invokeMatchingRoute = function(route) {
                var regex = new RegExp('^' + route + '$', 'i');
                if ( regex.test(req.url) ) {
                    middlewareByRoute[route](req, res, next);
                    return true;
                }

                return false;
            };

            if ( Object.keys(middlewareByRoute).some(invokeMatchingRoute) ) {
                return;
            }

            return next();
        },
        renderSync: function(baseDir) {
            // mock response object that tricks express into working
            var r = {
                app: {
                    set: function(a) { if ( a === 'view engine' ) { return 'jade'; } return undefined; },
                    enabled: function() { return false; },
                    cache: {},
                    dynamicViewHelpers: false
                },
                renderFile: function() { return this.partial.apply(this, Array.prototype.slice.call(arguments)); }
            };
            r.__proto__ = require('http').ServerResponse.prototype;

            function mkdirpSync(base, dir) {
                parts = dir.split(path.sep || '/'); // probably an old node version thing
                if ( parts[0].length === 0 ) {
                    // handle path = '/foo/bar'
                    parts.splice(0, 1);
                }

                parts.reduce(function(base, part) {
                    var p = path.join(base, part);
                    try {
                        fs.statSync(p);
                    } catch ( e ) {
                        fs.mkdirSync(p);
                    }

                    return p;
                }, base);
            }

            renderJs(r, function(err, data) { 
                if ( err ) {
                    console.error(err);
                }
                mkdirpSync(baseDir, baseRoute);
                var modelPath = path.join(baseDir, baseRoute, 'model.js');
                console.log(modelPath);
                fs.writeFileSync(modelPath, data);
            });
        }
    };
};

var getContentTypeForValue = function(value) {
    if ( value === null || value === undefined || _.isString(value) ) {
        return 'text/plain';
    }

    return 'application/json';
};

var getConnectedModelFunctions = function(model, getInstanceFunctions) {
    // figure out which methods we should expose to the front end
    if ( model instanceof Function ) {
        // model is a constructor
        model = getInstanceFunctions ? model.prototype : model;

        return Object.keys(model).map(function(modelPropertyName) {
            var modelProperty = model[modelPropertyName];
            if ( modelProperty instanceof Function && modelProperty.ajaxify ) {
                return modelPropertyName;
            }
        }).filter(function(modelPropertyName) { return !!modelPropertyName; });
    }

    throw Error('connected_model only handles functions as models');
};

var getParamsFromUrl = function(url, route) {
    var paramsStr = url.substring(route.length);
    if ( paramsStr[0] === '/' ) {
        paramsStr = paramsStr.substring(1);
    }
    return paramsStr.split('/').map(decodeURI)
           .filter(function(part) { return part.trim().length > 0; })
           .map(JSON.parse) || [];
};

/**
* Render `ctor` with the given `name`, skipping any properties in `propertiesToSkip`.
*
* @param {Function} ctor
* @param {String} name
* @param {Array} propertiesToSkip
* @return {String}
* @api private
*/
var renderClassDefinition = function(ctor, name, propertiesToSkip) {
    propertiesToSkip = propertiesToSkip || [];

    return ['var ' + name + ' = ' + toSource(ctor.prototype.constructor) + ';'].concat( 
        Object.keys(ctor.prototype).filter(function(key) {
            var prop = ctor.prototype[key];
            return !~propertiesToSkip.indexOf(key) && /^[^_].*/.exec(key) && !prop.hasOwnProperty('hide_from_client');
        }).map(function(key){
            var val = ctor.prototype[key];
            return name + '.prototype["' + key + '"] = ' + toSource(val) + ';';
        }).concat(
            Object.keys(ctor).filter(function(key) {
                var prop = ctor[key];
                return !~propertiesToSkip.indexOf(key) && /^[^_].*/.exec(key) && !prop.hasOwnProperty('hide_from_client');
            }).map(function(key) {
                var val = ctor[key];
                return name + '["' + key + '"] = ' + toSource(val) + ';';
            })
        )
    ).join('\n');
};

/**
* Return a javascript representation of `obj`.
*
* @param {Mixed} obj
* @return {String}
* @api private
*/
var toSource = function(obj) {
  if (_.isFunction(obj)) {
    return obj.toString();
  } else if (_.isDate(obj)) {
    return 'new Date("' + obj + '")';
  } else if (_.isArray(obj)) {
    return '[' + obj.map(toSource).join(', ') + ']';
  } else if ('[object Object]' === Object.prototype.toString.call(obj)) {
    return '{' + Object.keys(obj).map(function(key){
      return '"' + key + '":' + toSource(obj[key]);
    }).join(', ') + '}';
  } else {
    return JSON.stringify(obj);
  }
};
