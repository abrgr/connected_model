var mysql = require('db-mysql'),
    P = require('bluebird'),
    utils = require('./utils'),
    _ = require('underscore');

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

function ninvoke(obj, method) {
    var args = Array.prototype.slice.call(arguments, 2);

    return new P(function(resolve, reject) {
        function cb(err, data) {
            if ( err ) {
                return reject(err);
            }

            return resolve(data);
        }

        obj[method].apply(obj, args.concat(cb));
    });
}

/**
* Create a mysql-backed model for the given `model`, where `options` describes the table structure for model objects.
*
* @param {Function} model - Constructor for a model class
* @param {Object} options - Object describing table structure for given class
**/
var MySqlModel = module.exports = function(model, pool, options) {
    var log = module.exports.log;

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

    function namespaceField(tableName, field) {
        return tableName + '.' + field;
    }

    function namespaceFields(tableName, fields) {
        return fields.map(namespaceField.bind(null, tableName));
    }

    function getAliasedField(nsField) {
        return nsField.replace('.', '_');
    }

    function getAliasedFields(nsFields) {
        return nsFields.map(function(nsField) {
            var aliasedField = {};
            aliasedField[getAliasedField(nsField)] = nsField;
            return aliasedField;
        });
    }

    function getAliasedAndNamespacedFields(tableName, fields) {
        return getAliasedFields(namespaceFields(tableName, fields));
    }

    function reverseObject(obj) {
        var reversed = {};
        Object.keys(obj).forEach(function(k) {
            reversed[obj[k]] = k;
        });
        return reversed;
    }

    model._pool = pool;
    var joins = Object.keys(options.fields).filter(function(key) { return !!options.fields[key].join; });
    var allFields = model._allFields = Object.keys(options.fields).map(function(key) { return options.fields[key].field; });
    var tableName = model._tableName = options.table;
    var idProperties = model._idProperties = Object.keys(options.fields)
                        .filter(function(key) { return options.fields[key].id; });
    var idFields = model._idFields = idProperties.map(function(idProperty) { return options.fields[idProperty].field; });
    var requiredJoinFields = Object.keys(options.fields).filter(function(key) { return options.fields[key].requiredJoinField; })
                                .map(function(key) { return options.fields[key].field; });
    var localRequiredJoinProperties = Object.keys(options.fields).filter(function(property) { return options.fields[property].requiredJoinField; });
    var manyToManies = model._manyToManies = options.manyToManies;
    var manyToManyProperties = Object.keys(manyToManies);
    var deleteMarkerProperties = Object.keys(options.fields).filter(function(property) { return options.fields[property].deleteMarker; })
    var deleteMarkerProperty;
    if ( deleteMarkerProperties.length > 1 ) {
        throw new Error('Multiple delete markers specified for ' + tableName);
    } else if ( deleteMarkerProperties.length === 0 ) {
        log.info('No delete marker specified for [', tableName, ']');
    } else {
        deleteMarkerProperty = deleteMarkerProperties[0];
        log.info('Using delete marker [', deleteMarkerProperty, '] for [', tableName, ']');
    }
    var updateTimestampProperties = Object.keys(options.fields).filter(function(property) { return options.fields[property].updateTimestamp; });
    var updateTimestampProperty;
    if ( updateTimestampProperties.length > 1 ) {
        throw new Error('Multiple update timestamp properties specified for ' + tableName);
    } else if ( updateTimestampProperties.length === 1 ) {
        updateTimestampProperty = updateTimestampProperties[0];
        log.info('Using update timestamp [', updateTimestampProperty, '] for [', tableName, ']');
    }
    var updateForDuplicate = options.updateForDuplicate;

    var joinedModels = joins.map(function(property) { return options.fields[property].join; }).concat(_.values(manyToManies).map(function(m) { return m.thatModel; }));

    var localNSFieldsByAlias = _.extend.apply(_, [{}].concat(getAliasedAndNamespacedFields(tableName, allFields)));
    var recursiveNSFieldsByAlias = _.extend.apply(_, [{}, localNSFieldsByAlias]
                                                        .concat(_.pluck(joinedModels, '_recursiveNSFieldsByAlias')));
    model._recursiveNSFieldsByAlias = recursiveNSFieldsByAlias;
    var recursiveAliasesByNSField = model._recursiveAliasesByNSField = reverseObject(recursiveNSFieldsByAlias);

    var localAliasedFieldsByNSField = reverseObject(localNSFieldsByAlias);
    var localAliasedFieldNamesByPropertyName = _.extend.apply(_, [{}].concat(Object.keys(options.fields).map(function(propertyName) {
            var ret = {};
            ret[propertyName] = localAliasedFieldsByNSField[namespaceField(tableName, options.fields[propertyName].field)];
            return ret;
        })));
    var recursiveAliasedFieldNamesByPropertyName = _.extend.apply(_, [{}, localAliasedFieldNamesByPropertyName]
                                                                     .concat(_.pluck(joinedModels, '_recursiveAliasedFieldNamesByPropertyName')));
    model._recursiveAliasedFieldNamesByPropertyName = recursiveAliasedFieldNamesByPropertyName;
    var localPropertyNamesByAliasedFieldName = reverseObject(localAliasedFieldNamesByPropertyName);
    var recursivePropertyNamesByAliasedFieldName = reverseObject(recursiveAliasedFieldNamesByPropertyName);
    model._recursivePropertyNamesByAliasedFieldName = recursivePropertyNamesByAliasedFieldName;
    var fieldNamesByPropertyName = _.extend.apply(_, [{}].concat(Object.keys(options.fields).map(function(propertyName) {
            var ret = {};
            ret[propertyName] = options.fields[propertyName].field; 
            return ret;
        })));
    model._fieldNamesByPropertyName = fieldNamesByPropertyName;
    var propertyNamesByFieldName = reverseObject(fieldNamesByPropertyName);

    var localJoins = joins.map(function(property) {
        var j = options.fields[property];
        return {
            type: j.joinType || 'inner', // TODO: we need to turn this into a left outer if it's included as part of a join (e.g. a --LEFT OUTER--> b --INNER--> c should turn into a --LEFT OUTER--> b --LEFT OUTER-->c
            table: j.join._tableName,
            conditions: j.join._idFields.map(function(joinIdField) {
                // TODO: this doesn't scale to multiple ids
                return namespaceField('`' + tableName + '`', '`' + j.field + '`') + '=' + namespaceField('`' + j.join._tableName + '`', '`' + joinIdField + '`');
            }).join(' and ')
        };
    }).concat(_.flatten(manyToManyProperties.map(function(property) {
        var m = options.manyToManies[property];
        return [
            // first the mapping table
            {
                type: 'left outer',
                table: m.joinTable,
                conditions: Object.keys(m.thisFields).map(function(thisProperty) {
                    return namespaceField('`' + tableName + '`', '`' + fieldNamesByPropertyName[thisProperty] + '`') + '=' + namespaceField('`' + m.joinTable + '`', '`' + m.thisFields[thisProperty] + '`');
                }).join(' and ')
            },
            // now the target table
            {
                type: 'left outer',
                table: m.thatModel._tableName,
                conditions: Object.keys(m.thatFields).map(function(thatProperty) {
                    return namespaceField(m.thatModel._tableName, m.thatModel._fieldNamesByPropertyName[thatProperty]) 
                                + '=' + namespaceField(m.joinTable, m.thatFields[thatProperty]);
                }).join(' and ')
            }
        ];
    })));

    var recursiveJoins = _.flatten(localJoins.concat(_.pluck(joinedModels, '_recursiveJoins')));
    model._recursiveJoins = recursiveJoins;

    /**
     * Takes a query and calls query.join for each join we should make
     **/
    function addJoins(query) {
        if ( recursiveJoins.length > 0 ) {
            recursiveJoins.forEach(function(join) { query.join(join, []); });
        }

        return query; 
    }

    /**
     * Allow objects to define a _toSqlValue method to control their serialization
     **/
    var toSqlValue = function(val) {
        if ( !_.isUndefined(val) && val !== null && _.isFunction(val._toSqlValue) ) {
            return val._toSqlValue();
        }

        if ( val instanceof Date ) {
            // we use the utc value (implied by iso), but we remove the timezone specifier
            // since mysql is not timezone-aware (in our usage).
            return val.toISOString().replace('Z', '');
        }

        return val;
    };

    /**
     * Takes an object of model type and returns an array of {field, fieldValue} objects
     * representing all of the properties of obj, as well as the properties of joined/contained
     * objects.
     *
     * If forUpdate is set and we have an updateTimestampProperty, we will also include
     * a {field, fieldValue} for the updateTimestampProperty, set to now.
     **/
    var getFieldValues = model._getFieldValues = function(obj, forUpdate) {
        if ( forUpdate ) {
            // include the updateTimstampProperty
            var updateTimestampSetter = {};
            updateTimestampSetter[updateTimestampProperty] = new Date();
            obj = _.extend({}, updateTimestampSetter, obj);
        }

        var ret = _.flatten(_.compact(Object.keys(localNSFieldsByAlias).map(function(alias) {
            var property = localPropertyNamesByAliasedFieldName[alias];
            if ( _.isUndefined(obj[property]) || options.fields[property].join ) {
                return null;
            }

            return {
                field: recursiveNSFieldsByAlias[alias],
                fieldValue: toSqlValue(obj[property])
            };
        }))).concat(_.flatten(_.compact(joins.map(function(property) {
            if ( options.fields[property] && options.fields[property].join && obj[property] ) {
                // n-to-one
                var alias = localAliasedFieldNamesByPropertyName[property];

                return { field: localNSFieldsByAlias[alias],
                         fieldValue: obj[property][options.fields[property].join._idProperties[0]] };
            } else if ( manyToManies[property] ) {
                // many to many
                // we don't support searching on many-to-manies
            }

            return null;
        }))));
        return ret;
    };

    function getDb(context, cb) {
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
                if ( db ) {
                    try {
                        log.info('Destroying db connection after error');
                        pool.destroy(db);
                    } catch ( destroyErr ) {
                        log.error("Failed to release db connection after error", destroyErr);
                    }
                }

                return pool.create(function(dbOrError) {
                    if ( dbOrError instanceof Error ) {
                        log.error("CRITICAL DATABASE ERROR - could not connect after error", dbOrError);
                    }
                    log.info("Created new db connection after error");

                    return pool.release(dbOrError);
                });
            }

            return pool.release(db);
        }

        return new P(function(resolve, reject) {
            function cleanupSuccess(db, val) {
                releaseDb(null, db);
                return resolve(val);
            }

            function cleanupFailure(db, err) {
                log.error('Error executing db routine', err);
                releaseDb(err, db);
                return reject(err);
            }

            if ( !!context && !!context._dbConnection ) {
                log.info('Re-using connection');
                return cb.call(context, context._dbConnection).then(resolve).catch(cleanupFailure.bind(null, context._dbConnection));
            } else {
                log.info('Acquiring new connection from pool');
                pool.acquire(function(err, db) {
                    if ( !!err ) {
                        return reject(err);
                    }

                    if ( !!context ) {
                        context._dbConnection = db;
                    }

                    cb.call(context, db).then(cleanupSuccess.bind(null, db)).catch(cleanupFailure.bind(null, db));
                });
            }
        });
    }

    var getIds = function(model) {
        var passedInIds = idProperties.map(function(idProperty) { return model[idProperty]; });
        if ( !(passedInIds.every(function(id) { return _.isNumber(id) || _.isString(id); })) ) {
            return false;
        }

        return passedInIds;
    };

    function requiredJoinMatcher(model) {
        return _.object(
            idProperties.concat(localRequiredJoinProperties).map(function(prop) {
                return [ prop, model[prop] ];
            })
        );
    }

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

        if ( deleteMarkerProperty && !match.hasOwnProperty(deleteMarkerProperty) ) {
            // by default, we only look at non-deleted objects
            var deleteMarker = {};
            deleteMarker[deleteMarkerProperty] = 0;
            match = _.extend({}, match, deleteMarker);
        }

        var fieldValues = getFieldValues(match);

        localRequiredJoinProperties.forEach(function(requiredProperty) {
            var found = Object.keys(match).some(function(prop) {
                return prop === requiredProperty && !_.isUndefined(match[prop]);
            });

            if ( !found ) {
                throw new Error('Search [' + query.sql() + '] does not include required join field: [' + requiredProperty + ': ' + fieldNamesByPropertyName[requiredProperty] + ']');
            }
        });

        var wheresAndValues = [];
        fieldValues.forEach(function(fieldAndValue) {
            var fieldValue = fieldAndValue.fieldValue; 
            var fieldName = db.name(fieldAndValue.field); 

            if ( fieldValue !== null && 'object' === typeof(fieldValue) ) {
                // we have some special query
                return Object.keys(sqlForSpecialMatchConditions).forEach(function(matchCondition) {
                    var matchConditionValue = toSqlValue(fieldValue[matchCondition]),
                        sqlFragment;
                    if ( typeof matchConditionValue !== 'undefined' ) {
                        sqlFragment = sqlForSpecialMatchConditions[matchCondition];
                        if ( sqlFragment.indexOf('?') < 0 ) {
                            // 0-arg fragment
                            wheresAndValues.push({whereClause: fieldName + sqlFragment, value: undefined});
                        } else {
                            wheresAndValues.push({whereClause: fieldName + sqlFragment, value: matchConditionValue});
                        }
                    }
                });
            } 
            
            if ( null === fieldValue ) {
                // convenience for isNull
                var sqlFragment = sqlForSpecialMatchConditions.$isNull;
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

    var addOrderBys = model._addOrderBys = function(db, query, match) {
        if ( 'object' !== typeof(match) ) {
           throw new Error('match must be an object'); 
        }

        var fieldValues = getFieldValues(match, true);

        var orderBys = {};
        fieldValues.forEach(function(fieldAndValue) {
            var fieldValue = fieldAndValue.fieldValue; 
            var fieldName = fieldAndValue.field; 

            if ( fieldValue !== null && 'object' === typeof(fieldValue) ) {
                // we may have an order by
                if ( !!fieldValue.$orderAsc ) {
                    orderBys[fieldName] = true;
                }

                if ( !!fieldValue.$orderDesc ) {
                    orderBys[fieldName] = false;
                }
            } 
        });

        if ( Object.keys(orderBys).length > 0 ) {
            query = query.order(orderBys);
        }

        return query;
    };

    var singleRowToModel = model._singleRowToModel = function(row) {
        // create an object with the right properties
        var modelRow = {};

        Object.keys(row).forEach(function(alias) {
            var propName = localPropertyNamesByAliasedFieldName[alias];
            var field;
            if ( !_.isUndefined(propName) ) {
                modelRow[propName] = row[alias];
            }
        });

        // These joins are 1-to-1 with the underlying entity so we can do this here, while processing a single row
        joins.forEach(function(property) {
            modelRow[property] = options.fields[property].join._singleRowToModel(row);
        });

        return modelRow;
    };

    model._toModel = function(rows) {
        var rowsById = _.groupBy(rows, function(r) {
            return JSON.stringify(idProperties.map(function(idProp) {
                var fieldName = localAliasedFieldNamesByPropertyName[idProp];
                return r[fieldName];
            }));
        });

        return Object.keys(rowsById).map(function(modelId) {
            var rowsForId = rowsById[modelId];
            var modelRow = singleRowToModel(rowsForId[0]);

            // these joins are many-to-many with the underlying entity so we can't do this in singleRowToModel.  It has to be done here.
            manyToManyProperties.forEach(function(property) {
                if ( !modelRow[property] ) {
                    modelRow[property] = [];
                }
                Array.prototype.push.apply(modelRow[property], options.manyToManies[property].thatModel._toModel(rowsForId));
            });

            return new model(modelRow);
        });
    };

    function toModel(rows) {
        return P.resolve(model._toModel(rows));
    }

    model.get_by_id = function(ids, context) {
        if ( _.isNumber(ids) || _.isString(ids) ) {
            var id = ids;
            ids = {};
            ids[idProperties[0]] = id;
        } 

        return model.matchSingle(ids, context);
    };
    
    model.get_by_id.ajaxify = true;

    model.match = function(match, context) {
        if ( 'object' !== typeof(match) ) {
            return P.reject(Error('match must be an object.  It is a ' + typeof(match)));
        }

        return getDb(context, function(db) {
            return new P(function(resolve, reject) {
                var query = db.query().select(recursiveNSFieldsByAlias).from(tableName);
                query = addJoins(query);
                query = addWheres(db, query, match);
                query = addOrderBys(db, query, match);

                if ( !!match.$limit ) {
                    query = query.limit(match.$limit);
                }

                query.execute(function(error, rows, cols) {
                    if ( !!error ) {
                        return reject(new Error(error));
                    }

                    if ( rows.length < 1 ) {
                        return resolve([]);
                    }

                    toModel(rows).then(resolve).catch(reject);
                });
            });
        });
    };
    
    model.match.ajaxify = true;

    model.matchSingle = function(match, context) {
        return model.match(match, context).then(function(results) {
            if ( results.length > 1 ) {
                throw new Error('matchSingle returned multiple matches');
            }

            return results[0];
        });
    };

    model.matchSingle.ajaxify = true;

    model.queryFor = function(match, groupBy, context) {
        if ( !_.isObject(match) ) {
            return P.reject(Error('match must be an object.  It is a ' + typeof(match)));
        }

        if ( arguments.length === 2 && _.isObject(groupBy) ) {
            context = groupBy;
            groupBy = undefined;
        }

        return getDb(context, function(db) {
            return new P(function(resolve, reject) {
                var query = db.query();
                query = addSelects(query, match).from(tableName);
                query = addWheres(db, query, match);
                query = addOrderBys(db, query, match);
                if ( !!groupBy ) {
                    query = query.add(' group by ' + options.fields[groupBy].field + ' ');
                }

                query.execute(function(error, rows, cols) {
                    if ( !!error ) {
                        return reject(new Error(error));
                    }

                    if ( rows.length < 1 ) {
                        return resolve([]);
                    }

                    return resolve(rows);
                });
            });
        });
    };

    model.queryFor.ajaxify = true;

    model.queryForSingle = function(match, groupBy, context) {
        return model.queryFor(match, groupBy, context).then(function(results) {
            if ( results.length > 1 ) {
                throw new Error('queryForSingle returned multiple matches');
            }

            return results[0];
        });
    };

    model.queryForSingle.ajaxify = true;

    model.set = function(newData, match, context) {
        var self = this;

        return getDb(context, function(db) {
            return new P(function(resolve, reject) {
                var fieldsAndValues = getFieldValues(newData, true),
                    fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; }),
                    fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; }),
                    query = db.query().update(tableName).set(utils.objectZip(fieldNames, fieldValues));

                addWheres(db, query, match);

                query.execute(function(err, result) {
                    if ( !!err ) {
                        return reject(new Error(err));
                    }

                    if ( !!result.warnings ) {
                        // TODO: log warnings
                        return reject(new Error(result.warnings + ' Warnings in update'));
                    }

                    return resolve({ id: result.id });
                });
            });
        });
    };

    model.set.ajaxify = true;

    model.prototype.save = function() {
        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return P.reject(new Error('model is not valid'));
        }

        if ( !!getIds(this) ) {
            return this.update();
        } 

        return this.insert();
    };

    model.prototype.save.ajaxify = true;

    var inThere = function(arr, val) { return _.detect(arr, _.isEqual.bind(val)); };
                
    model.prototype.insert = function(userDefinedIds) {
        var self = this;

        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return P.reject(new Error('model is not valid'));
        }

        var passedInIds = getIds(this);
        if ( !userDefinedIds && !!passedInIds ) {
            log.warn('insert called on a model that already has an id', this);
        }

        return getDb(this, function(db) {
            return new P(function(resolve, reject) {
                var fieldsAndValues = getFieldValues(self, true);
                var fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; });
                var fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; });

                var query = db.query().insert(tableName, fieldNames, fieldValues);
                if ( updateForDuplicate ) {
                    query.add(' on duplicate key update ' + updateForDuplicate )
                }

                query.execute(function(err, result) {
                    if ( !!err ) {
                        return reject(new Error(err));
                    }

                    if ( !!result.warnings ) {
                        // TODO: log warnings
                        return reject(new Error(result.warnings + ' Warnings in insert'));
                    }

                    var didInsert = !!(0|result.affected);
                    if ( updateForDuplicate && !didInsert ) {
                        log.warn('Did not insert due to duplicate keys', self);

                        return resolve({}); // we don't have an id
                    }
                    
                    if ( idProperties.length === 1 ) {
                        //TODO: figure out how this should work for compound ids
                        self[idProperties[0]] = result.id;
                    }

                    return resolve({ id: result.id });
                });
            });
        });
    };

    model.prototype.insert.ajaxify = true;

    model.prototype.update = function() {
        var self = this;

        if ( _.isFunction(this.isValid) && !this.isValid() ) {
            return P.reject(new Error('model is not valid'));
        }

        var passedInIds = getIds(this);
        if ( !passedInIds ) {
            return P.reject(new Error('update called on a model that has no id'));
        }

        return getDb(this, function(db) {
            return new P(function(resolve, reject) {
                var requiredFieldNames = idFields.concat(requiredJoinFields),
                    requiredFieldValues = passedInIds.concat(requiredJoinFields.map(function(field){return self[field];})),
                    fieldsAndValues = getFieldValues(self, true),
                    fieldNames = fieldsAndValues.map(function(fieldValue) { return fieldValue.field; }),
                    fieldValues = fieldsAndValues.map(function(fieldValue) { return fieldValue.fieldValue; }),
                    query = db.query().update(tableName).set(utils.objectZip(fieldNames, fieldValues))
                      .where(requiredFieldNames.map(function(f){return f + '=?';}).join(' and '), requiredFieldValues);

                query.execute(function(err, result) {
                    if ( !!err ) {
                        return reject(new Error(err));
                    }

                    if ( !!result.warnings ) {
                        // TODO: log warnings
                        return reject(new Error(result.warnings + ' Warnings in update'));
                    }

                    return resolve({ id: result.id });
                });
            });
        });
    };

    model.prototype.update.ajaxify = true;

    model.prototype._reserveId = function() {
        var self = this;

        return getDb(null, function(db) {
            var ctx = this;

            function reserveId() {
                var idReserver = self.withoutIds(),
                    txnContext = {
                        _dbConnection: db,
                        _inTxn: true,
                        _txnName: 'reserveid'
                    };

                // we don't care if this is valid or not
                idReserver.isValid = function() { return true; };
                return idReserver.withTransaction(txnContext).insert();
            }

            if ( ctx._inTxn ) {
                // we'll use a savepoint but this should never happen
                return ninvoke(db.query(), 'execute', 'SAVEPOINT RESERVEID').then(reserveId).then(function(result) {
                    var id = result.id;
                    return ninvoke(db.query(), 'execute', 'ROLLBACK TO SAVEPOINT RESERVEID').return(id);
                });
            } else {
                // we'll create a transaction
                return ninvoke(db.query(), 'execute', 'START TRANSACTION').then(reserveId).then(function(result) {
                    var id = result.id;
                    return ninvoke(db.query(), 'execute', 'ROLLBACK').return(id);
                });
            }
        }).catch(function(err) {
            log.error('Failed to reserve id', err);
            throw err;
        });
    };


    model.prototype.delete = function() {
        var self = this,
            passedInIds = getIds(self);
        if ( !passedInIds ) {
            return P.reject(new Error('delete called on a model that has no id'));
        }

        return getDb(self, function(db) {
            function executeQueryPart(query) {
                query = addWheres(db, query, requiredJoinMatcher(self));

                return ninvoke(query, 'execute').then(function(result) {
                    if ( !!result.warnings ) {
                        // TODO: log warnings
                        throw new Error(result.warnings + ' Warnings in delete');
                    }

                    return;
                });
            }

            var fieldNames = idFields.concat(requiredJoinFields),
                fieldValues = passedInIds.concat(requiredJoinFields.map(function(field){return self[field];}));

            if ( deleteMarkerProperty ) {
                // we have a field that acts as a delete marker.  we never actually delete,
                // just set this field to a non-0 id
                return self._reserveId(self).then(function(deleteId) {
                    var deleteSetter = {};
                    deleteSetter[deleteMarkerProperty] = deleteId;

                    var deleteFieldsAndNames = getFieldValues(deleteSetter, true),
                        deleteFieldNames = _.pluck(deleteFieldsAndNames, 'field'),
                        deleteFieldValues = _.pluck(deleteFieldsAndNames, 'fieldValue');

                    return executeQueryPart(db.query().update(tableName).set(utils.objectZip(deleteFieldNames, deleteFieldValues)));
                });
            } else {
                // we don't have a delete marker, do a real delete
                return executeQueryPart(db.query().delete().from(tableName));
            }

        });
    };

    model.prototype.delete.ajaxify = true;

    model.query = function(sql, values, context) {
        values = values || [];

        return getDb(context, function(db) {
            return new P(function(resolve, reject) {
                return db.query().execute(sql, values, function(err, result) {
                    if ( !!err ) {
                        return reject(err);
                    }

                    return resolve(result);
                });
            });
        });
    };

    model.query.ajaxify = false;
    model.query.hide_from_client = true;

    model.prototype.query = function(sql, values) {
        return getDb(this, function(db) {
            return new P(function(resolve, reject) {
                return db.query().execute(sql, values, function(err, result) {
                    if ( !!err ) {
                        return reject(err);
                    }

                    return resolve(result);
                });
            });
        });
    };

    model.prototype.query.ajaxify = false;
    model.prototype.query.hide_from_client = true;

    /**
     * Creates a transaction in the db with the specified name.  Calls fn, with this set to the transaction context. fn must return a promise and the transaction will be committed
     * when that transaction is resolved or rolled back on rejection.
     * 
     * Usage: 
     * user.createTransaction('set user info', function() {
     *     return P.all([
     *         user.withTransaction(this).save(),
     *         User.set({name: name}, {uid: uid}, this),
     *         ...
     *     ]);
     * });
     *
     * @param {String} name - Name of transaction
     * @param {Function} fn - Function called after the transaction is created.
     * @return {Object} promise resolved after transaction finishes
     */
    function createTransaction(name, fn) {
        return new P(function(txnCompleteResolve, txnCompleteReject) {
            if ( arguments.length === 1 ) {
                fn = name;

                name = 'unknown';
            }

            // work finished is resolved when the body of the transaction is complete
            //    after this is resolved, we try to commit the transaction.  if it's rejected,
            //    we roll back.
            // txn complete is resolved once we commit or rejected once we roll back or fail to
            //    commit
            pool.acquire(function(err, db) {
                if ( !!err ) {
                    return txnCompleteReject(err);
                }

                log.info('Acquired db connection for transaction', name);

                var txnContext = 
                {
                    _dbConnection: db,
                    _inTxn: true,
                    _txnName: name
                };

                function endTxn(db, commit, data) {
                    if ( commit ) {
                        db.query().execute('COMMIT', function(err, result) {
                            pool.release(db);
                            if ( !!err ) {
                                log.error('Failed to commit transaction', name);
                                return txnCompleteReject(err);
                            }

                            log.trace('Committed transaction', name);

                            return txnCompleteResolve(data);
                        });
                    } else {
                        db.query().execute('ROLLBACK', function(err, result) {
                            pool.release(db);
                            if ( !!err ) {
                                log.error('Failed to roll back transaction', name);
                                return txnCompleteReject(err);
                            }

                            return txnCompleteReject(data);
                        });
                    }
                }

                db.query().execute('START TRANSACTION', function(err, result) {
                    if ( !!err ) {
                        log.error('Failed to start transaction', name);
                        return txnCompleteReject(err);
                    }

                    log.trace('Started transaction', name);

                    return fn.call(txnContext).then(endTxn.bind(null, db, true)).catch(endTxn.bind(null, db, false));
                });
            });
        });
    };

    model.prototype.createTransaction = createTransaction;

    model.prototype.createTransaction.ajaxify = false;
    model.prototype.createTransaction.hide_from_client = true;

    model.createTransaction = createTransaction;

    model.createTransaction.ajaxify = false;
    model.createTransaction.hide_from_client = true;

    model.prototype.withTransaction = function(txnContext) {
        this._dbConnection = txnContext._dbConnection;
        this._inTxn = txnContext._inTxn;

        return this;
    };

    model.prototype.withTransaction.ajaxify = false;
    model.prototype.withTransaction.hide_from_client = false;

    return model;
};
