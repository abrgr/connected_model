var mysql = require('db-mysql');
var utils = require('./utils');
var Deferred = require('deferred');
var _ = require('underscore');

var sqlForSpecialMatchConditions = {'$gt': '>?', '$gte': '>=?', '$lt': '<?', '$lte': '<=?', '$ne': '<>?', '$likeAnywhere': " like concat('%', ?, '%')"}; 
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

    if ( !options.table ) {
        throw new Error('No table was specified on options');
    }

    if ( !options.fields ) {
        throw new Error('No fields specified on options');
    }

    options.manyToManies = options.manyToManies || {};

    var joins = Object.keys(options.fields).filter(function(key) { return !!options.fields[key].join; });
    var allFields = model._allFields = Object.keys(options.fields).map(function(key) { return options.fields[key].field; });
    var tableName = model._tableName = options.table;
    var idProperties = model._idProperties = Object.keys(options.fields)
                        .filter(function(key) { return options.fields[key].id; });
    var idFields = model._idFields = idProperties.map(function(idProperty) { return options.fields[idProperty].field; });
    var allNonIdFields = allFields.filter(function(field) { return !~idFields.indexOf(field); });
    var requiredJoinFields = Object.keys(options.fields).filter(function(key) { return options.fields[key].requiredJoin; })
                                .map(function(key) { return options.fields[key].field; });
    var propertyNamesByFieldName = model._propertyNamesByFieldName = {};
    Object.keys(options.fields).forEach(function(propertyName) { propertyNamesByFieldName[options.fields[propertyName].field] = propertyName; });

    var manyToManyProperties = Object.keys(options.manyToManies);

    var getJoins = function() { 
        return Object.keys(options.fields).filter(function(key) { return !!options.fields[key].join; }).map(function(key) {
            var j = options.fields[key];
            return {
                type: j.joinType || 'inner',
                table: j.join._tableName,
                conditions: j.join._idFields.map(function(joinIdField) {
                    return tableName + '.' + j.field + '=' + j.join._tableName + '.' + joinIdField;
                }).join(' and ')
            };
        });
    };

    var addJoins = model._addJoins = function(query) {
        var joins = getJoins();
        if ( joins.length > 0 ) {
            joins.map(function(join) { query.join(join, []); });
        }

        return query; 
    };

    var namespaceFields = function(tableName, fields) {
        return fields.map(function(field) { return tableName + '.' + field; });
    };

    var getFields = model._getFields = function(skipNamespaces, thisTableOnly) {
        skipNamespaces = skipNamespaces === undefined ? false : skipNamespaces;
        thisTableOnly = thisTableOnly === undefined ? false : thisTableOnly;
        var fields = skipNamespaces ? allFields : namespaceFields(tableName, allFields);
        if ( !thisTableOnly ) {
            joins.forEach(function(propertyName) {
                var join = options.fields[propertyName];
                fields = fields.concat(join.join._getFields(skipNamespaces));
            });
        }

        return fields;
    };

    var toSqlValue = function(val) {
        if ( _.isFunction(val._toSqlValue) ) {
            return val._toSqlValue();
        }

        return val;
    };

    var getFieldValues = function(obj, thisTableOnly) {
        thisTableOnly = thisTableOnly === undefined ? false : thisTableOnly;
        return getFields(true, thisTableOnly).filter(function(fieldName){return !!obj[propertyNamesByFieldName[fieldName]];})
                          .map(function(fieldName){
            var key = propertyNamesByFieldName[fieldName];
            var field = options.fields[key];
            if ( !!field.join ) {
                return field.join._idProperties.map(function(joinIdProperty) {
                    return {field: field.join._tableName + '.' + field.field, fieldValue: toSqlValue(obj[key][joinIdProperty])};
                });
            }

            return [{field: tableName + '.' + field.field, fieldValue: toSqlValue(obj[key])}];
        }).reduce(function(a, b) { return a.concat(b); }, []);
    };

    var getDb = function(context, cb) {
        var deferred = new Deferred();

        pool.acquire(function(err, db) {
            if ( !!err ) {
                return deferred.reject(err);
            }

            deferred.guard(context, cb, db, deferred); 
        });

        return deferred.promise();
    };

    var getId = function(model) {
        var passedInIds = idProperties.map(function(idProperty) { return model[idProperty]; });
        if ( !(passedInIds.every(function(id) { return _.isNumber(id) || _.isString(id); })) ) {
            return false;
        }

        return passedInIds;
    };

    var addSelects = function(query, match) {
        var targetKeys = Object.keys(match).filter(function(key) {
            var tgt = match[key];
            return 'object' === typeof(tgt) && Object.keys(tgt).some(function(k) { return !!sqlForQueries[k]; });
        });

        var sqlFragments = [];
        targetKeys.forEach(function(targetKey) {
            var target = match[targetKey];
            var queries = Object.keys(sqlForQueries).filter(function(q) { return !!target[q]; });
            queries.forEach(function(query) {
                sqlFragments.push(sqlForQueries[query].replace(FIELD_NAME, options.fields[targetKey].field) + ' AS ' + target[query]);
            });
        });

        return query.select(sqlFragments.join(','));
    };

    var addWheres = model._addWheres = function(db, query, match) {
        if ( 'object' !== typeof(match) ) {
           throw new Error('match must be an object'); 
        }

        var fieldValues = getFieldValues(match, true);

        requiredJoinFields.forEach(function(requiredField) {
            var found = fieldValues.some(function(actualFieldValue) {
                return actualFieldValue.field === requiredField;
            });

            if ( !found ) {
                throw new Error('Search does not include required join field: [' + requiredField + ']');
            }
        });

        var wheresAndValues = [];
        fieldValues.forEach(function(fieldAndValue) {
            var fieldValue = fieldAndValue.fieldValue; 
            var fieldName = db.name(fieldAndValue.field); 

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

    var getManyToManyValues = function(db, obj, manyToManyProperty) {
        var manyToMany = options.manyToManies[manyToManyProperty];
        /* manyToMany looks like this:
           {
            joinTable: 'meds_regimen_dose',
            thisFields: {'id': 'meds_regimen_id'},
            thatModel: Prescription,
            thatFields: {'id': 'meds_id'}
           }
        */
        var thatModel = manyToMany.thatModel;

        var intermediateQuery = db.query().select(_.values(manyToMany.thatFields))
                                          .from(manyToMany.joinTable)
                                          .where(_.values(manyToMany.thisFields).map(function(fld){return fld + '=?';}).join(' and '), 
                                                 Object.keys(manyToMany.thisFields).map(function(prop){return obj[prop];}));

        var d = new Deferred();

        intermediateQuery.execute(function(error, rows, cols) {
            if ( !!error ) {
                return d.reject(error);
            }

            return Deferred.afterAll(rows.map(function(row) {
                var innerDeferred = new Deferred();

                var matchObj = {};
                Object.keys(manyToMany.thatFields).forEach(function(prop) {
                  matchObj[prop] = row[manyToMany.thatFields[prop]];
                });

                manyToMany.thatModel.match(matchObj).success(function(results) {
                    innerDeferred.resolve({property: manyToManyProperty, values: results});
                }).fail(innerDeferred.chainedReject);

                return innerDeferred.promise();
            })).success(function(inners){d.resolve(_.flatten(inners));}).fail(d.chainedReject);
        });

        return d.promise();
    };

    var fillInManyToManies = model._fillInManyToManies = function(db, obj) {
        var deferred = new Deferred();

        var manyToManyDeferreds = manyToManyProperties.map(getManyToManyValues.bind(null, db, obj));

        Deferred.afterAll(manyToManyDeferreds).success(function(results) {
            results = _.flatten(results);
            results.forEach(function(result) {
                obj[result.property] = (obj[result.property] || []).concat(result.values);
            });

            return deferred.resolve(obj);
        }).fail(deferred.chainedReject);

        return deferred;
    };

    var singleRowToModel = model._singleRowToModel = function(db, row) {
        var d = new Deferred();

        // create an object with the right properties
        var modelRow = new model();
        var promises = [];
        var propNameByPromise = {};
        Object.keys(row).forEach(function(fieldName) {
            var propName = propertyNamesByFieldName[fieldName];
            if ( propName === undefined ) {
                // ignore fields we don't know about
                return;
            }

            var field = options.fields[propName];
            if ( !!field.join ) {
                var promise = field.join._singleRowToModel(db, row);
                promises.push(promise);
                propNameByPromise[promise] = propName;
            } else {
                modelRow[propName] = row[fieldName];
            }
        });

        Deferred.afterAll([fillInManyToManies(db, modelRow)].concat(promises)).success(function(props) {
            props = _.flatten(props.slice(1)); // take the tail of props--the first entry is for the many to manies
            props.forEach(function(prop) {
                modelRow[propNameByPromise[prop]] = prop;
            });

            return d.resolve(modelRow);
        }).fail(d.chainedReject);

        return d.promise();
    };

    var toModel = model._toModel = function(db, rows) {
        var d = new Deferred();
        Deferred.afterAll(rows.map(singleRowToModel.bind(null, db))).success(function(results) {
            return d.resolve(_.flatten(results));
        }).fail(d.chainedReject);

        return d.promise();
    };

    model.get_by_id = function(ids) {
        if ( _.isNumber(ids) || _.isString(ids) ) {
            var id = ids;
            ids = {};
            ids[idFields[0]] = id;
        } 

        return this.matchSingle(ids);
    };
    
    model.get_by_id.ajaxify = true;

    model.match = function(match) {
        if ( 'object' !== typeof(match) ) {
            return Deferred.rejected(Error('match must be an object.  It is a ' + typeof(match)));
        }

        return getDb(this, function(db, deferred) {
            deferred.guard(this, function() {
                var query = db.query().select(getFields()).from(tableName);
                addJoins(query);
                query = addWheres(db, query, match);

                query.execute(function(error, rows, cols) {
                    if ( !!error ) {
                        return deferred.reject(new Error(error));
                    }

                    if ( rows.length < 1 ) {
                        return deferred.resolve([]);
                    }

                    toModel(db, rows).success(function(matches) {
                        deferred.resolve(matches);
                        return pool.release(db);
                    }).fail(function(err) {
                        deferred.reject(err);
                        return pool.release(db);
                    });
                });
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
        }).fail(d.chainedReject);

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
            query = addWheres(db, query, match);

            query.execute(function(error, rows, cols) {
                pool.release(db);

                if ( !!error ) {
                    return deferred.reject(new Error(error));
                }

                if ( rows.length < 1 ) {
                    return deferred.resolve([]);
                }

                return deferred.resolve(rows);
            });
        });
    };

    model.queryFor.ajaxify = true;

    model.queryForSingle = function(match) {
        var d = new Deferred();

        model.queryFor(match).success(function(results) {
            if ( results.length > 1 ) {
                return d.reject(new Error('queryForSingle returned multiple matches'));
            }

            return d.resolve(results[0]);
        }).fail(d.chainedReject);

        return d.promise();
    };

    model.queryForSingle.ajaxify = true;

    model.set = function(newData, match) {
        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldsAndValues = getFieldValues(newData, true);
            var fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; });
            var fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; });
            var query = db.query().update(tableName).set(utils.objectZip(fieldNames, fieldValues));
            addWheres(db, query, match);
            query.execute(function(err, result) {
                if ( !!err ) {
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in update'));
                }

                return deferred.resolve({id: result.id});
              });
        });
    };

    model.set.ajaxify = true;

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

    model.prototype.saveAssociations = function() {
        return getDb(this, function(db, deferred) {
            // TODO: this assumes we don't want dups written.  That should be an option, not an assumption.
            // first, retrieve all the existing many-to-many values
            var manyToManyDeferreds = manyToManyProperties.map(getManyToManyValues.bind(null, db, obj));
            Deferred.afterAll(manyToManyDeferreds).success(function(results) {
                results = _.flatten(results);
                var insertAndDeleteDeferreds = [];

                results.forEach(function(result) {
                    var property = result.property;
                    var currentVals = result.values;
                    var newVals = obj[property];

                    var inThere = function(arr, val) { return _.detect(arr, _.isEqual.bind(val)); };
                    var toDelete = _.reject(currentVals, inThere.bind(null, newVals));
                    var toAdd = _.reject(newVals, inThere.bind(null, currentVals));

                    var deleteDeferreds = toDelete.map(function(deleteMe){deleteMe.delete();});

                    var thatModel = options.manyToManies[property].thatModel;
                    var addDeferreds = toAdd.map(function(addMe) { thatModel.prototype.insert(); });

                    insertAndDeleteDeferreds = insertAndDeleteDeferreds.concat(deleteDeferreds).concat(addDeferreds);
                });

                return Deferred.afterAll(insertAndDeleteDeferreds).success(deferred.chainedResolve).fail(deferred.chainedReject);
            }).fail(deferred.chainedReject);
        });
    };

    model.prototype.insert = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        var passedInIds = getId(this);
        if ( !!passedInIds ) {
            return Deferred.rejected(new Error('insert called on a model that already has an id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldsAndValues = getFieldValues(self, true);
            var fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; });
            var fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; });

            db.query().insert(tableName, fieldNames, fieldValues)
              .execute(function(err, result) {
                if ( !!err ) {
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in insert'));
                }
                
                if ( idProperties.length === 1 ) {
                    //TODO: figure out how this should work for compound ids
                    self[idProperties[0]] = result.id;
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

        var passedInIds = getId(this);
        if ( !passedInIds ) {
            return Deferred.rejected(new Error('update called on a model that has no id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var requiredFieldNames = idFields.concat(requiredJoinFields);
            var requiredFieldValues = passedInIds.concat(requiredJoinFields.map(function(field){return self[field];}));
            var fieldsAndValues = getFieldValues(self, true);
            var fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; });
            var fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; });
            var query = db.query().update(tableName).set(utils.objectZip(fieldNames, fieldValues))
              .where(requiredFieldNames.map(function(f){return f + '=?';}).join(' and '), requiredFieldValues);
            query.execute(function(err, result) {
                if ( !!err ) {
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in update'));
                }

                return deferred.resolve({id: result.id});
              });
        });
    };

    model.prototype.update.ajaxify = true;

    model.prototype.delete = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        var passedInIds = getIds(this);
        if ( !passedInIds ) {
            return Deferred.rejected(new Error('delete called on a model that has no id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldNames = idFields.concat(requiredJoinFields);
            var fieldValues = passedInIds.concat(requiredJoinFields.map(function(field){return self[field];}));
            db.query().delete(tableName).where(fieldNames.map(function(f){return f + '=?';}).join(' and '), fieldValues)
              .execute(function(err, result) {
                if ( !!err ) {
                    return deferred.reject(new Error(err));
                }

                if ( !!result.warnings ) {
                    // TODO: log warnings
                    return deferred.reject(new Error(result.warnings + ' Warnings in delete'));
                }

                return deferred.resolve({id: result.id});
              });
        });
    };

    model.prototype.delete.ajaxify = true;

    return model;
};
