var mysql = require('db-mysql');
var utils = require('./utils');
var Deferred = require('./deferred');
var _ = require('underscore');

/**
* Create a mysql-backed model for the given `model`, where `options` describes the table structure for model objects.
*
* @param {Function} model - Constructor for a model class
* @param {Object} options - Object describing table structure for given class
**/
var MySqlModel = module.exports = function(model, pool, options) {
    if ( !_.isFunction(model) ) {
        throw new Error('Model must be a constructor');
    }

    if ( arguments.length === 2 ) {
        // pool was given as an option
        options = pool;
        pool = options.pool;
    }

    if ( arguments.length === 1 ) {
        // pool and model were given as options
        options = model;
        model = options.model;
        pool = options.pool;
    }

    if ( !model ) {
        throw new Error('No model was specified');
    }

    if ( !pool ) {
        throw new Error('No pool was specified');
    }

    var allFields = Object.keys(options.fields).map(function(key) { return options.fields[key].field; });
    var tableName = options.table;
    var idField = Object.keys(options.fields).map(function(key) { return options.fields[key]; })
                        .filter(function(fieldInfo) { return fieldInfo.id; } )
                        .map(function(fieldInfo) { return fieldInfo.field; })[0]; // TODO: support compound ids
    var allNonIdFields = allFields.filter(function(field) { return idField !== field; });

    var getDb = function(context, cb) {
        var deferred = new Deferred();

        pool.acquire(function(err, db) {
            if ( !!err ) {
                deferred.reject(new Error(err));
                return;
            }

            try {
                cb.call(context, db, deferred);
            } catch ( error ) {
                deferred.reject(error);
                return;
            }
        });

        return deferred.promise();
    };

    var getId = function(model) {
        var passedInId = model[idField];
        if ( !(_.isNumber(passedInId) || _.isString(passedInId)) ) {
            return false;
        }

        return passedInId;
    };

    model.get_by_id = function(id) {
        if ( !(_.isNumber(id) || _.isString(id)) ) {
            return Deferred.rejected(Error('id must be a number or string.  It is a ' + typeof(id)));
        }

        return getDb(null, function(db, deferred) {
            db.query().select(allFields).from(tableName).where(idField + '=?', [id]).execute(function(error, rows, cols) {
                pool.release(db);

                if ( !!error ) {
                    deferred.reject(new Error(error));
                    return;
                }

                if ( rows.length > 1 ) {
                    deferred.reject(new Error('get_by_id should never return multiple records'));
                    return;
                }

                if ( rows.length < 1 ) {
                    deferred.resolve(null);
                    return;
                }

                deferred.resolve(utils.classify(rows[0], model));
            });
        });
    };
    
    model.get_by_id.ajaxify = true;

    model.prototype.save = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        if ( !!getId(this) ) {
            return this.update();
        } 

        return this.insert();
    };

    model.prototype.save.ajaxify = true;

    model.prototype.insert = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        var passedInId = getId(this);
        if ( !!passedInId ) {
            return Deferred.rejected(new Error('insert called on a model that already has an id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldValues = allNonIdFields.map(function(field){return self[field];});
            db.query().insert(tableName, allNonIdFields, fieldValues)
              .execute(function(err, result) {
                if ( !!err ) {
                    deferred.reject(new Error(err));
                    return;
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    deferred.reject(new Error(result.warnings + ' Warnings in save'));
                    return;
                }

                deferred.resolve({id: result.id});
              });
        });
    };

    model.prototype.insert.ajaxify = true;

    model.prototype.update = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        var passedInId = getId(this);
        if ( !passedInId ) {
            return Deferred.rejected(new Error('update called on a model that has no id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldValues = allNonIdFields.map(function(field){return self[field];});
            db.query().update(tableName).set(utils.objectZip(allNonIdFields, fieldValues))
              .where(idField + '=?', [passedInId])
              .execute(function(err, result) {
                if ( !!err ) {
                    deferred.reject(new Error(err));
                    return;
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    deferred.reject(new Error(result.warnings + ' Warnings in save'));
                    return;
                }

                deferred.resolve({id: result.id});
              });
        });
    };

    model.prototype.update.ajaxify = true;

    return model;
};
