var mysql = require('db-mysql'),
    utils = require('./utils'),
    Deferred = require('deferred'),
    _ = require('underscore'),
    log = require('log4js').getLogger('connected-model.mysql-model');

var sqlForSpecialMatchConditions = 
    {
        '$gt': '>?',
        '$gte': '>=?', 
        '$lt': '<?', 
        '$lte': '<=?', 
        '$ne': '<>?', 
        '$likeAnywhere': " like concat('%', ?, '%')",
        '$in': ' in (?)',
        '$isNull': ' is null',
        '$notNull': ' is not null'
    }; 
var FIELD_NAME = '__FIELD_NAME__';
var sqlForQueries = {'$min': 'min(' + FIELD_NAME + ')', 
                     '$max': 'max(' + FIELD_NAME + ')', 
                     '$avg': 'avg(' + FIELD_NAME + ')',
                     '$count': 'count(' + FIELD_NAME + ')',
                     '$val': FIELD_NAME
                    };

function getObjectValues(obj) { return Object.keys(obj).map(function(k) { return obj[k]; }); }

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

    model._pool = pool;
    var joins = Object.keys(options.fields).filter(function(key) { return !!options.fields[key].join; });
    var allFields = model._allFields = Object.keys(options.fields).map(function(key) { return options.fields[key].field; });
    var tableName = model._tableName = options.table;
    var idProperties = model._idProperties = Object.keys(options.fields)
                        .filter(function(key) { return options.fields[key].id; });
    var idFields = model._idFields = idProperties.map(function(idProperty) { return options.fields[idProperty].field; });
    var allNonIdFields = allFields.filter(function(field) { return !~idFields.indexOf(field); });
    var requiredJoinFields = Object.keys(options.fields).filter(function(key) { return options.fields[key].requiredJoinField; })
                                .map(function(key) { return options.fields[key].field; });
    var propertyNamesByFieldName = model._propertyNamesByFieldName = {};
    Object.keys(options.fields).forEach(function(propertyName) { propertyNamesByFieldName[options.fields[propertyName].field] = propertyName; });
    var manyToManies = model._manyToManies = options.manyToManies;
    var manyToManyProperties = Object.keys(manyToManies);

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

    var getAliasedFields = function(fields) {
        return fields.map(function(field) {
            var aliasedField = {};
            aliasedField[field.replace('.', '_')] = field;
            return aliasedField;
        });
    };

    var toSqlValue = function(val) {
        if ( !_.isUndefined(val) && val !== null && _.isFunction(val._toSqlValue) ) {
            return val._toSqlValue();
        }

        return val;
    };

    var getFieldValues = function(obj, thisTableOnly, skipNamespaces) {
        thisTableOnly = thisTableOnly === undefined ? false : thisTableOnly;
        skipNamespaces = skipNamespaces === undefined ? false : skipNamespaces;
        return getFields(true, thisTableOnly).filter(function(fieldName){return !_.isUndefined(obj[propertyNamesByFieldName[fieldName]]);})
                          .map(function(fieldName){
            var key = propertyNamesByFieldName[fieldName];
            var field = options.fields[key];
            if ( !!field.join ) {
                return field.join._idFields.map(function(joinIdField) {
                    if ( thisTableOnly ) {
                        return {field: skipNamespaces ? field.field : tableName + '.' + field.field,
                                fieldValue: toSqlValue(obj[key][field.join._propertyNamesByFieldName[joinIdField]])};
                    }
                    return {field: sipNamespaces ? joinIdField : field.join._tableName + '.' + joinIdField, 
                            fieldValue: toSqlValue(obj[key][field.join._propertyNamesByFieldName[joinIdField]])};
                });
            }

            return [{field: skipNamespaces ? field.field : tableName + '.' + field.field,
                     fieldValue: toSqlValue(obj[key])}];
        }).reduce(function(a, b) { return a.concat(b); }, []);
    };

    var getDb = function(context, cb) {
        var deferred = new Deferred();

        function releaseDb(error, db) {
            if ( !!context && !!context._inTxn ) {
                log.info('Not releasing connection to pool--in txn');
                return;
            }

            log.info('Releasing connection to pool');
            if ( !!context ) {
                context._dbConnection = null;
            }

            if ( !!error ) {
                log.info('DB error occurred.  Creating new connection');

                // assume the worst case.  throw away this connection and get a new one.
                return pool.create(function(dbOrError) {
                    if ( dbOrError instanceof Error ) {
                        log.error("CRITICAL DATABASE ERROR - could not connect after error", dbOrError);
                    }
                    log.info("GOT NEW DB");

                    return pool.release(dbOrError);
                });
            }

            return pool.release(db);
        }

        if ( !!context && !!context._dbConnection ) {
            log.info('Re-using connection');
            deferred.guard(context, cb, context._dbConnection, deferred); 
        } else {
            log.info('Acquiring new connection from pool');
            pool.acquire(function(err, db) {
                if ( !!err ) {
                    return deferred.reject(err);
                }

                if ( !!context ) {
                    context._dbConnection = db;
                }

                deferred.success(releaseDb.bind(null, undefined, db)).fail(function(error) {
                    log.error('Error executing db routine', arguments);

                    return releaseDb(error, db);
                });

                deferred.guard(context, cb, db, deferred); 
            });
        }

        return deferred.promise();
    };

    var getIds = function(model) {
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
            requiredField = tableName + '.' + requiredField;
            var found = fieldValues.some(function(actualFieldValue) {
                return actualFieldValue.field === requiredField;
            });

            if ( !found ) {
                throw new Error('Search [' + query.sql() + '] does not include required join field: [' + requiredField + ']');
            }
        });

        var wheresAndValues = [];
        fieldValues.forEach(function(fieldAndValue) {
            var fieldValue = fieldAndValue.fieldValue; 
            var fieldName = db.name(fieldAndValue.field); 

            if ( fieldValue !== null && 'object' === typeof(fieldValue) ) {
                // we have some special query
                return Object.keys(sqlForSpecialMatchConditions).forEach(function(matchCondition) {
                    var sqlFragment = sqlForSpecialMatchConditions[matchCondition];
                    var matchConditionValue = toSqlValue(fieldValue[matchCondition]);
                    if ( !!matchConditionValue ) {
                        if ( matchCondition === '$isNull' || matchCondition === '$notNull' ) { // TODO: pretty hackish.  put this in some nicer config for 0-arg conditions
                            wheresAndValues.push({whereClause: fieldName + sqlFragment, value: undefined});
                        } else {
                            wheresAndValues.push({whereClause: fieldName + sqlFragment, value: matchConditionValue});
                        }
                    }
                });
            } 
            
            if ( null === fieldValue ) {
                // convenience for isNull
                var sqlFragment = sqlForSpecialMatchConditions['$isNull'];
                return wheresAndValues.push({whereClause: fieldName + sqlFragment, value: undefined});
            }

            // we have a normal field = value query
            return wheresAndValues.push({whereClause: fieldName + '=?', value: fieldValue});
        });

        if ( wheresAndValues.length > 0 ) {
            var firstWhereAndValue = wheresAndValues[0];
            var remainingWheresAndValues = wheresAndValues.slice(1);

            query = query.where(firstWhereAndValue.whereClause, _.isUndefined(firstWhereAndValue.value) ? [] : [firstWhereAndValue.value]);
            remainingWheresAndValues.forEach(function(whereAndValue) {
                query = query.and(whereAndValue.whereClause, _.isUndefined(whereAndValue.value) ? [] : [whereAndValue.value]);
            });
        }

        return query;
    };

    var getManyToManyValues = function(db, obj, manyToManyProperty) {
        var manyToMany = manyToManies[manyToManyProperty];
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

        return deferred.promise();
    };

    var singleRowToModel = model._singleRowToModel = function(db, row) {
        var d = new Deferred();

        // create an object with the right properties
        var modelRow = new model();
        var promises = [];
        var propNameByPromise = {};
        var fieldsByAlias = {};
        _.extend.apply(_, [fieldsByAlias].concat(getAliasedFields(getFields())));

        Object.keys(row).forEach(function(alias) {
            var fieldName = fieldsByAlias[alias];
            var fieldTableName = undefined;
            if ( !!fieldName ) { 
                // if we have a qualified field, un-qualify it.
                var aliasParts = fieldName.split('.');
                fieldName = aliasParts[1]; 
                fieldTableName = aliasParts[0];
            }

            var propName = propertyNamesByFieldName[fieldName];
            var field = undefined;
            if ( _.isUndefined(propName) ) {
                // check joins - we only care about one level here since we have built in recursion below
                matchingJoinProps = joins.filter(function(joinProperty) { 
                    return options.fields[joinProperty].join._propertyNamesByFieldName[fieldName]; 
                });
                if ( matchingJoinProps.length > 1 ) {
                    // TODO: really bad
                }
                propName = matchingJoinProps[0];
                field = options.fields[propName];
            } else {
                field = options.fields[propName];
            }

            if ( _.isUndefined(propName) ) {
                // just skip properties we don't know about
                return;
            }

            if ( !!field && !!field.join ) {
                var promise = field.join._singleRowToModel(db, row);
                promises.push(promise);
                propNameByPromise[promise] = propName;
            } else if ( fieldTableName === tableName ) {
                // alias is a field on this model
                modelRow[propName] = row[alias];
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

    model.get_by_id = function(ids, context) {
        if ( _.isNumber(ids) || _.isString(ids) ) {
            var id = ids;
            ids = {};
            ids[propertyNamesByFieldName[idFields[0]]] = id;
        } 

        return model.matchSingle(ids, context);
    };
    
    model.get_by_id.ajaxify = true;

    model.match = function(match, context) {
        if ( 'object' !== typeof(match) ) {
            return Deferred.rejected(Error('match must be an object.  It is a ' + typeof(match)));
        }

        return getDb(context, function(db, deferred) {
            var aliasedFields = getAliasedFields(getFields());
            var query = db.query().select(aliasedFields).from(tableName);
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
                    return deferred.resolve(matches);
                }).fail(function(err) {
                    return deferred.reject(err);
                });
            });
        });
    };
    
    model.match.ajaxify = true;

    model.matchSingle = function(match, context) {
        var d = new Deferred();

        model.match(match, context).success(function(results) {
            if ( results.length > 1 ) {
                return d.reject(new Error('matchSingle returned multiple matches'));
            }

            return d.resolve(results[0]);
        }).fail(d.chainedReject);

        return d.promise();
    };

    model.matchSingle.ajaxify = true;

    model.queryFor = function(match, groupBy, context) {
        if ( !_.isObject(match) ) {
            return Deferred.rejected(Error('match must be an object.  It is a ' + typeof(match)));
        }

        if ( arguments.length === 2 && _.isObject(groupBy) ) {
            context = groupBy;
            groupBy = undefined;
        }

        return getDb(context, function(db, deferred) {
            var query = db.query();
            query = addSelects(query, match).from(tableName);
            query = addWheres(db, query, match);
            if ( !!groupBy ) {
                query = query.add(' group by ' + options.fields[groupBy].field + ' ');
            }

            query.execute(function(error, rows, cols) {
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

    model.queryForSingle = function(match, groupBy, context) {
        var d = new Deferred();

        model.queryFor(match, groupBy, context).success(function(results) {
            if ( results.length > 1 ) {
                return d.reject(new Error('queryForSingle returned multiple matches'));
            }

            return d.resolve(results[0]);
        }).fail(d.chainedReject);

        return d.promise();
    };

    model.queryForSingle.ajaxify = true;

    model.set = function(newData, match, context) {
        return getDb(context, function(db, deferred) {
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

        if ( !!getIds(this) ) {
            return this.update();
        } 

        return this.insert();
    };

    model.prototype.save.ajaxify = true;

    var inThere = function(arr, val) { return _.detect(arr, _.isEqual.bind(val)); };
                
    model.prototype.saveAssociations = function() {
        return getDb(this, function(db, deferred) {
            var self = this;

            // TODO: this assumes we don't want dups written.  That should be an option, not an assumption.
            // first, retrieve all the existing many-to-many values
            var manyToManyDeferreds = manyToManyProperties.map(getManyToManyValues.bind(null, db, this));
            var insertAndDeleteDeferreds = [];
            Deferred.afterAll(manyToManyDeferreds).success(function(results) {
                results = _.flatten(results);

                var resultsAndProperties = results.map(function(result) { return {property: result.property, currentVals: result.values}; });
                manyToManyProperties.forEach(function(property) {
                    var currentVals = _(resultsAndProperties.filter(function(rAndP) { return rAndP.property === property; }))
                                                             .pluck('currentVals')
                                                             .reduce(function(a, b){return a.concat(b);}, []);

                    var newVals = self[property];

                    var toDelete = _.reject(currentVals, inThere.bind(null, newVals));
                    var toAdd = _.reject(newVals, inThere.bind(null, currentVals));

                    var manyToMany = manyToManies[property];
                    var thatModel = manyToMany.thatModel;
                    var deleteDeferreds = toDelete.map(function(deleteMe){ 
                        return {delete: true, manyToMany: manyToMany, result: result};
                    });
                    var addDeferreds = toAdd.map(function(addMe) { 
                        var d = new Deferred();
                        thatModel.prototype.save.call(addMe).success(function(result) {
                            d.resolve({insert: true, manyToMany: manyToMany, result: result});
                        }).fail(d.chainedReject);
                        return d.promise();
                    });

                    insertAndDeleteDeferreds = insertAndDeleteDeferreds.concat(deleteDeferreds).concat(addDeferreds);
                });
            }).fail(deferred.chainedReject);

            Deferred.afterAll(insertAndDeleteDeferreds).success(function(results) {
                results = _(results).flatten();
                results.map(function(result) {
                    var id = result.result.id;
                    var manyToMany = result.manyToMany;
                    var thisFields = getObjectValues(manyToMany.thisFields);
                    var thatFields = getObjectValues(manyToMany.thatFields);
                    var manyToManyFields = thisFields.concat(thatFields);
                    var thisFieldsValues = thisFields.map(function(fld){return this[fld];});
                    if ( result.insert ) {
                        // TODO: we can only support a single join-to id.  What happens if we have multiple?
                        db.query().insert(manyToMany.joinTable, manyToManyFields, thisFieldsValues.concat(id));
                    } else {
                        // delete
                        db.query().delete(manyToMany.joinTable)
                                  .where(manyToManyFields.map(function(fld){return fld + '=?';}).join(' AND '), thisFieldsValues.concat(id));
                    }
                });
            });
        });
    };

    model.prototype.saveAssociations.ajaxify = false;
    model.prototype.saveAssociations.hide_from_client = false;

    model.prototype.insert = function(userDefinedIds) {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return Deferred.rejected(new Error('model is not valid'));
        }

        var passedInIds = getIds(this);
        if ( !userDefinedIds && !!passedInIds ) {
            log.info('insert called on a model that already has an id', this);
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var fieldsAndValues = getFieldValues(self, true, true);
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

        var passedInIds = getIds(this);
        if ( !passedInIds ) {
            return Deferred.rejected(new Error('update called on a model that has no id'));
        }

        return getDb(this, function(db, deferred) {
            var self = this;
            var requiredFieldNames = idFields.concat(requiredJoinFields);
            var requiredFieldValues = passedInIds.concat(requiredJoinFields.map(function(field){return self[field];}));
            var fieldsAndValues = getFieldValues(self, true, true);
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
            db.query().delete().from(tableName).where(fieldNames.map(function(f){return f + '=?';}).join(' and '), fieldValues)
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

    model.query = function(sql, values, context) {
        var values = values || [];

        return getDb(context, function(db, deferred) {
            return db.query().execute(sql, values, function(err, result) {
                if ( !!err ) {
                    return deferred.reject(err);
                }

                return deferred.resolve(Array.prototype.slice.call(arguments, 1));
            });
        });
    };

    model.query.ajaxify = false;
    model.query.hide_from_client = true;

    model.prototype.query = function(sql, values) {
        return getDb(this, function(db, deferred) {
            return db.query().execute(sql, values, function(err, result) {
                if ( !!err ) {
                    return deferred.reject(err);
                }

                return deferred.resolve(Array.prototype.slice.call(arguments, 1));
            });
        });
    };

    model.prototype.query.ajaxify = false;
    model.prototype.query.hide_from_client = true;

    /**
     * Creates a transaction in the db with the specified name.  Calls fn, with this set to the transaction context, passing in the deferred object returned
     * by createTransaction.
     * 
     * Usage: 
     * user.createTransaction('set user info', function(deferred) {
     *     return deferred.afterAll([
     *         user.withTransaction(this).save(),
     *         User.set({name: name}, {uid: uid}, this),
     *         ...
     *     ]);
     * }).success(...).fail(...);
     *
     * @param {String} name - Name of transaction (arguments.caller.name is used if none is supplied)
     * @param {Function} fn - Function called after the transaction is created.
     * @return {Object} Deferred that should be resolved by the inner function
     */
    model.prototype.createTransaction = function(name, fn) {
        if ( arguments.length === 1 ) {
            fn = name;

            name = arguments.caller.name;
        }

        var self = this;

        var deferred = new Deferred();

        pool.acquire(function(err, db) {
            if ( !!err ) {
                return deferred.reject(err);
            }

            log.info('Acquired db connection for transaction', name);

            var txnContext = 
            {
                _dbConnection: db,
                _inTxn: true,
                _txnName: name
            };

            function endTxn(db, commit) {
                if ( commit ) {
                    db.query().execute('COMMIT', function(err, result) {
                        pool.release(db);
                        if ( !!err ) {
                            log.error('Failed to commit transaction', name);
                        }

                        log.trace('Committed transaction', name);

                        return;
                    });
                } else {
                    db.query().execute('ROLLBACK', function(err, result) {
                        pool.release(db);
                        if ( !!err ) {
                            log.error('Failed to roll back transaction', name);
                        }

                        log.trace('Rolled back transaction', name);

                        return;
                    });
                }
            }

            deferred.success(endTxn.bind(null, db, true)).fail(endTxn.bind(null, db, false));

            db.query().execute('START TRANSACTION', function(err, result) {
                if ( !!err ) {
                    log.error('Failed to start transaction', name);
                    return deferred.reject(err);
                }

                log.trace('Started transaction', name);

                return deferred.guard(txnContext, fn, deferred); 
            });
        });

        return deferred.promise();
    };

    model.prototype.createTransaction.ajaxify = false;
    model.prototype.createTransaction.hide_from_client = true;

    model.prototype.withTransaction = function(txnContext) {
        this._dbConnection = txnContext._dbConnection;
        this._inTxn = txnContext._inTxn;

        return this;
    };

    model.prototype.withTransaction.ajaxify = false;
    model.prototype.withTransaction.hide_from_client = false;

    return model;
};
