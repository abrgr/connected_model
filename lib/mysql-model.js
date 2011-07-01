var mysql = require('db-mysql');
var utils = require('./utils');
var Deferred = require('deferred');
var _ = require('underscore');

var sqlForSpecialMatchConditions = {'$gt': '>?', '$gte': '>=?', '$lt': '<?', '$lte': '<=?', '$ne': '<>?'}; 
var FIELD_NAME = '__FIELD_NAME__';
var sqlForQueries = {'$min': 'min(' + FIELD_NAME + ')', 
                     '$max': 'max(' + FIELD_NAME + ')', 
                     '$avg': 'avg(' + FIELD_NAME + ')'
                    };

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
                return deferred.reject(err);
            }

            try {
                cb.call(context, db, deferred);
            } catch ( error ) {
                return deferred.reject(error);
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

    var addSelects = function(query, match) {
        var targetKeys = Object.keys(match).filter(function(key) {
            return Object.keys(match[key]).some(function(k) { return !!sqlForQueries[k]; });
        });

        var sqlFragments = [];
        targetKeys.forEach(function(targetKey) {
            var target = match[targetKey];
            var queries = Object.keys(sqlForQueries).filter(function(q) { return !!target[q]; });
            queries.forEach(function(query) {
                sqlFragments.push(sqlForQueries[query].replace(FIELD_NAME, options.fields[targetKey].field));
            });
        });

        return query.select(sqlFragments.join(','));
    };

    var addWheres = function(query, match) {
        if ( 'object' !== typeof(match) ) {
           throw new Error('match must be an object'); 
        }

        var fieldValues = Object.keys(match).filter(function(key) {
            return match[key] !== undefined && !!options.fields[key];
        }).map(function(key) {
            return {field: options.fields[key].field, value: match[key]};  
        });

        var wheresAndValues = [];
        fieldValues.forEach(function(fieldAndValue) {
            var fieldValue = fieldAndValue.value; 
            var fieldName = fieldAndValue.field; 

            if ( 'object' === typeof(fieldValue) ) {
                // we have some special query
                return Object.keys(sqlForSpecialMatchConditions).forEach(function(matchCondition) {
                    var sqlFragment = sqlForSpecialMatchConditions[matchCondition];
                    var matchConditionValue = fieldValue[matchCondition];
                    if ( !!matchConditionValue ) {
                        wheresAndValues.push({whereClause: fieldName + sqlFragment, value: matchConditionValue});
                    }
                });
            }

            // we have a normal field = value query
            return wheresAndValues.push({whereClause: fieldName + '=?', value: fieldValue});
        });

        if ( wheresAndValues.length > 0 ) {
            var firstWhereAndValue = wheresAndValues[0];
            var remainingWheresAndValues = wheresAndValues.slice(1);

            query = query.where(firstWhereAndValue.whereClause, [firstWhereAndValue.value]);
            remainingWheresAndValues.forEach(function(whereAndValue) {
                query = query.and(whereAndValue.whereClause, [whereAndValue.value]);
            });
        }

        return query;
    };

    model.get_by_id = function(id) {
        if ( !(_.isNumber(id) || _.isString(id)) ) {
            return Deferred.rejected(Error('id must be a number or string.  It is a ' + typeof(id)));
        }

        return getDb(null, function(db, deferred) {
            db.query().select(allFields).from(tableName).where(idField + '=?', [id]).execute(function(error, rows, cols) {
                pool.release(db);

                if ( !!error ) {
                    return deferred.reject(new Error(error));
                }

                if ( rows.length > 1 ) {
                    return deferred.reject(new Error('get_by_id should never return multiple records'));
                }

                if ( rows.length < 1 ) {
                    return deferred.resolve(null);
                }

                return deferred.resolve(utils.classify(rows[0], model));
            });
        });
    };
    
    model.get_by_id.ajaxify = true;

    model.match = function(match) {
        if ( 'object' !== typeof(match) ) {
            return Deferred.rejected(Error('match must be an object.  It is a ' + typeof(match)));
        }

        return getDb(this, function(db, deferred) {
            var query = db.query().select(allFields).from(tableName);
            query = addWheres(query, match);
            
            query.execute(function(error, rows, cols) {
                pool.release(db);

                if ( !!error ) {
                    return deferred.reject(new Error(error));
                }

                if ( rows.length < 1 ) {
                    return deferred.resolve([]);
                }

                return deferred.resolve(rows.map(function(row) { return utils.classify(row, model); } ));
            });
        });
    };
    
    model.match.ajaxify = true;

    model.matchSingle = function(match) {
        var d = new Deferred();

        model.match(match).success(function(results) {
            if ( results.length > 1 ) {
                return d.reject(new Error('matchSingle returned multiple matches'));
            }

            return d.resolve(results[0]);
        }).fail(d.reject);

        return d.promise();
    };

    model.matchSingle.ajaxify = true;

    model.queryFor = function(match) {
        if ( 'object' !== typeof(match) ) {
            return Deferred.rejected(Error('match must be an object.  It is a ' + typeof(match)));
        }

        return getDb(this, function(db, deferred) {
            var query = db.query();
            query = addSelects(query, match).from(tableName);
            query = addWheres(query, match);
            
            query.execute(function(error, rows, cols) {
                pool.release(db);

                if ( !!error ) {
                    return deferred.reject(new Error(error));
                }

                if ( rows.length < 1 ) {
                    return deferred.resolve([]);
                }

                return deferred.resolve(rows.map(function(row) { return utils.classify(row, model); } ));
            });
        });
    };

    model.queryFor.ajaxify = true;

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
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in save'));
                }

                return deferred.resolve({id: result.id});
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
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in save'));
                }

                return deferred.resolve({id: result.id});
              });
        });
    };

    model.prototype.update.ajaxify = true;

    return model;
};
